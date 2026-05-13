"""Connector — the per-microservice runtime.

Each Python service constructs one ``Connector``, registers subscriptions
with :meth:`subscribe`, and starts it via :meth:`startup`.

Wire pattern (mirrors fabric@v1.27.1):

* On startup, the connector subscribes to its own response subject
  ``subjectOfResponses(plane, hostname, instance_id)`` so callers can
  receive ACKs and final responses.
* :meth:`request` publishes to the target subject **without** a NATS
  reply subject. ``Microbus-From-Host`` and ``Microbus-From-Id`` headers
  in the request tell the responder where to send back the ACK and the
  final response.
* Responders publish a 202 ACK frame immediately on receipt (before any
  authorization or handler execution) so callers can distinguish "no
  responders" (ACK timeout → 404) from "responder accepted, response
  pending".
* Authorization: missing or invalid actor → 401; valid actor whose
  claims don't satisfy ``required_claims`` → 403.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import sys
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

from microbus_py.cache.distrib import DistribCache
from microbus_py.claims.evaluator import compile_expr
from microbus_py.connector.route import parse_route
from microbus_py.decorators.config import _registry as _config_registry
from microbus_py.decorators.config import config_decorator
from microbus_py.decorators.event import event_decorator
from microbus_py.decorators.event_sink import event_sink_decorator
from microbus_py.decorators.function import function_decorator
from microbus_py.decorators.metric import _registry as _metric_registry
from microbus_py.decorators.metric import metric_decorator
from microbus_py.decorators.task import task_decorator
from microbus_py.decorators.ticker import (
    _start_tickers,
    _stop_tickers,
    _use_clock,
    ticker_decorator,
)
from microbus_py.decorators.web import web_decorator
from microbus_py.decorators.workflow import workflow_decorator
from microbus_py.errors.http import HTTP_REASONS, json_error_body
from microbus_py.errors.traced import TracedError
from microbus_py.frame.actor import (
    Actor,
    UnknownActorKeyError,
    parse_actor,
    public_key_from_jwk,
    token_issuer,
)
from microbus_py.frame.frame import Frame
from microbus_py.frame.headers import H, OpCode
from microbus_py.log import emit_log
from microbus_py.openapi.builder import build_openapi
from microbus_py.trace import (
    TraceRuntime,
    client_span,
    create_trace_runtime,
    inject_traceparent,
    server_span,
)
from microbus_py.wire.codec import (
    HTTPRequest,
    HTTPResponse,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
)
from microbus_py.wire.fragments import split
from microbus_py.wire.subjects import (
    request_subject,
    response_subject,
    subscription_subject,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    from pydantic import BaseModel

    from microbus_py.decorators.config import ConfigGetter
    from microbus_py.decorators.event import EventTrigger
    from microbus_py.decorators.event_sink import SinkHandler
    from microbus_py.decorators.function import FunctionFeature
    from microbus_py.decorators.metric import MetricRecorder
    from microbus_py.decorators.task import TaskHandler
    from microbus_py.decorators.ticker import TickerHandler
    from microbus_py.decorators.web import WebFeature
    from microbus_py.decorators.workflow import WorkflowFn
    from microbus_py.mock.time import VirtualClock
    from microbus_py.transport.base import IncomingMessage, Subscription, Transport
    from microbus_py.workflow.registry import TaskFeature, WorkflowFeature

__all__ = ["Connector", "Handler"]


Handler = Callable[[HTTPRequest], Awaitable[HTTPResponse]]
StartupCallback = Callable[[], Awaitable[None]]


_log = logging.getLogger("microbus_py.connector")
_DEFAULT_ACK_TIMEOUT = 0.3
_DEFAULT_SHUTDOWN_GRACE = 20.0
_FRAGMENT_DEADLINE_SECONDS = 60.0
_FRAGMENT_HEADROOM = 64 << 10
_MAX_FRAGMENTS = 1024
_MAX_BODY_BYTES = 64 * 1024 * 1024
_ALLOWED_DEPLOYMENTS = frozenset({"TESTING", "LOCAL", "LAB", "PROD"})
_MIN_FRAGMENT_SIZE = 64 << 10
_HANDLER_FAILURES: tuple[type[Exception], ...] = (Exception,)
_CURRENT_REQUEST_HEADERS: ContextVar[list[tuple[str, str]] | None] = ContextVar(
    "microbus_py_current_request_headers",
    default=None,
)
_PROPAGATED_EXACT_HEADERS = {"Accept-Language", H.CLOCK_SHIFT, H.ACTOR}
_PROPAGATED_PREFIXES = ("X-Forwarded-", H.BAGGAGE_PREFIX)
_RESPONSE_FRAME_HEADERS = {H.MSG_ID, H.FROM_HOST, H.FROM_ID, H.OP_CODE, H.FRAGMENT}


@dataclass(slots=True)
class _Subscription:
    name: str
    method: str
    path: str
    handler: Handler
    queue: str | None
    required_claims: str | None
    description: str
    port: int
    host_override: str | None
    sub_handles: list[Subscription] = field(default_factory=list)


@dataclass(slots=True)
class _Pending:
    ack: asyncio.Future[tuple[str, str]]
    response: asyncio.Future[bytes]


@dataclass(slots=True)
class _RequestFragments:
    total: int
    deadline_seconds: float = _FRAGMENT_DEADLINE_SECONDS
    chunks: dict[int, bytes] = field(default_factory=dict)
    first: HTTPRequest | None = None
    started_at: float = field(default_factory=time.monotonic)

    def add(self, req: HTTPRequest) -> HTTPRequest | None:
        frame = Frame(dict(req.headers))
        idx, total = frame.fragment
        if total != self.total:
            raise ValueError("request fragment total mismatch")
        if total > _MAX_FRAGMENTS:
            raise ValueError(f"request fragment total {total} exceeds limit {_MAX_FRAGMENTS}")
        if idx not in self.chunks:
            self.chunks[idx] = req.body
        if idx == 1 and self.first is None:
            self.first = req
        if len(self.chunks) < self.total or self.first is None:
            return None
        body = b"".join(self.chunks[i] for i in range(1, self.total + 1))
        if len(body) > _MAX_BODY_BYTES:
            raise ValueError(
                f"reassembled request body {len(body)} exceeds limit {_MAX_BODY_BYTES}"
            )
        return HTTPRequest(
            method=self.first.method,
            url=self.first.url,
            headers=_integrated_headers(self.first.headers, len(body)),
            body=body,
        )

    def expired(self) -> bool:
        return (time.monotonic() - self.started_at) > self.deadline_seconds


@dataclass(slots=True)
class _ResponseFragments:
    total: int
    deadline_seconds: float = _FRAGMENT_DEADLINE_SECONDS
    chunks: dict[int, bytes] = field(default_factory=dict)
    first: HTTPResponse | None = None
    started_at: float = field(default_factory=time.monotonic)

    def add(self, resp: HTTPResponse) -> HTTPResponse | None:
        frame = Frame(dict(resp.headers))
        idx, total = frame.fragment
        if total != self.total:
            raise ValueError("response fragment total mismatch")
        if total > _MAX_FRAGMENTS:
            raise ValueError(f"response fragment total {total} exceeds limit {_MAX_FRAGMENTS}")
        if idx not in self.chunks:
            self.chunks[idx] = resp.body
        if idx == 1 and self.first is None:
            self.first = resp
        if len(self.chunks) < self.total or self.first is None:
            return None
        body = b"".join(self.chunks[i] for i in range(1, self.total + 1))
        if len(body) > _MAX_BODY_BYTES:
            raise ValueError(
                f"reassembled response body {len(body)} exceeds limit {_MAX_BODY_BYTES}"
            )
        return HTTPResponse(
            status_code=self.first.status_code,
            reason=self.first.reason,
            headers=_integrated_headers(self.first.headers, len(body)),
            body=body,
        )

    def expired(self) -> bool:
        return (time.monotonic() - self.started_at) > self.deadline_seconds


def _integrated_headers(headers: list[tuple[str, str]], body_len: int) -> list[tuple[str, str]]:
    out = [
        (name, value)
        for name, value in headers
        if name != H.FRAGMENT and name.lower() != "content-length"
    ]
    if body_len:
        out.append(("Content-Length", str(body_len)))
    return out


def _issuer_host(issuer: str) -> str:
    parts = urlsplit(issuer)
    return parts.hostname or issuer


def _fragment_headers(
    headers: list[tuple[str, str]],
    idx: int,
    total: int,
) -> list[tuple[str, str]]:
    out = [
        (name, value)
        for name, value in headers
        if name != H.FRAGMENT and name.lower() != "content-length"
    ]
    if total > 1:
        out.append((H.FRAGMENT, f"{idx}/{total}"))
    return out


def _fragment_request(req: HTTPRequest, max_size: int) -> list[HTTPRequest]:
    chunks = split(req.body, max_size=max_size)
    total = len(chunks)
    if total == 1:
        return [req]
    return [
        HTTPRequest(
            method=req.method,
            url=req.url,
            headers=_fragment_headers(req.headers, idx, total),
            body=chunk,
        )
        for idx, chunk in enumerate(chunks, start=1)
    ]


def _fragment_response(resp: HTTPResponse, max_size: int) -> list[HTTPResponse]:
    chunks = split(resp.body, max_size=max_size)
    total = len(chunks)
    if total == 1:
        return [resp]
    return [
        HTTPResponse(
            status_code=resp.status_code,
            reason=resp.reason,
            headers=_fragment_headers(resp.headers, idx, total),
            body=chunk,
        )
        for idx, chunk in enumerate(chunks, start=1)
    ]


_CLIENT_SPAN_BODY_PEEK_LIMIT = 64 << 10


def _enrich_client_span_from_body(span: Any, body: bytes) -> None:
    if not body or len(body) > _CLIENT_SPAN_BODY_PEEK_LIMIT:
        return
    try:
        parsed = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return
    if not isinstance(parsed, dict):
        return
    run_id = parsed.get("run_id")
    if isinstance(run_id, str) and run_id:
        span.set_attribute("microbus.body.run_id", run_id)


def _inbound_propagated_headers() -> list[tuple[str, str]]:
    headers = _CURRENT_REQUEST_HEADERS.get()
    if not headers:
        return []
    return [
        (name, value)
        for name, value in headers
        if name in _PROPAGATED_EXACT_HEADERS
        or any(name.startswith(prefix) for prefix in _PROPAGATED_PREFIXES)
    ]


_parse_route = parse_route


def _normalize_actor_keys(
    keys: (Mapping[str, Ed25519PublicKey] | Mapping[str, Mapping[str, Ed25519PublicKey]] | None),
) -> dict[str, dict[str, Ed25519PublicKey]]:
    """Accept either flat {kid: key} or nested {iss: {kid: key}}.

    Flat form is stored under a wildcard issuer ``"*"`` for backward
    compatibility. The nested form is recommended because it enables
    per-issuer key namespacing in the connector.
    """
    if not keys:
        return {}
    sample = next(iter(keys.values()))
    if isinstance(sample, Mapping):
        nested = cast("Mapping[str, Mapping[str, Ed25519PublicKey]]", keys)
        return {iss: dict(inner) for iss, inner in nested.items()}
    flat = cast("Mapping[str, Ed25519PublicKey]", keys)
    return {"*": dict(flat)}


class Connector:
    """Per-microservice runtime."""

    def __init__(
        self,
        hostname: str,
        *,
        version: int = 0,
        description: str = "",
        name: str = "",
        package: str = "",
        db: str = "",
        cloud: str = "",
        instance_id: str | None = None,
        plane: str | None = None,
        locality: str | None = None,
        deployment: str = "TESTING",
        transport: Transport,
        ack_timeout: float = _DEFAULT_ACK_TIMEOUT,
        shutdown_grace: float = _DEFAULT_SHUTDOWN_GRACE,
        actor_public_keys: (
            Mapping[str, Ed25519PublicKey] | Mapping[str, Mapping[str, Ed25519PublicKey]] | None
        ) = None,
        trusted_issuer_hosts: Sequence[str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not hostname:
            raise ValueError("hostname must be non-empty")
        if shutdown_grace <= 0:
            raise ValueError("shutdown grace must be positive")
        if deployment not in _ALLOWED_DEPLOYMENTS:
            raise ValueError(
                f"deployment must be one of {sorted(_ALLOWED_DEPLOYMENTS)}, got {deployment!r}"
            )
        if deployment == "TESTING" and os.environ.get("MICROBUS_ALLOW_TESTING") != "1":
            raise RuntimeError(
                "deployment='TESTING' disables JWT signature verification. "
                "Set MICROBUS_ALLOW_TESTING=1 to enable in test contexts, "
                "or pass a non-TESTING deployment (LOCAL, LAB, PROD)."
            )
        self._hostname = hostname.lower()
        self._version = version
        self._description = description
        self._name = name
        self._package = package
        self._db = db
        self._cloud = cloud
        self._instance_id = instance_id or secrets.token_hex(8)
        self._plane = plane or "microbus"
        self._locality = locality or ""
        self._deployment = deployment
        self._transport = transport
        self._ack_timeout = ack_timeout
        self._shutdown_grace = shutdown_grace
        self._actor_public_keys: dict[str, dict[str, Ed25519PublicKey]] = _normalize_actor_keys(
            actor_public_keys
        )
        self._trusted_issuer_hosts: frozenset[str] = (
            frozenset(trusted_issuer_hosts) if trusted_issuer_hosts else frozenset()
        )
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(UTC))
        self._started = False
        self._subs: list[_Subscription] = []
        self._infra_subs: list[_Subscription] = []
        self._on_startup: list[StartupCallback] = []
        self._on_shutdown: list[StartupCallback] = []
        self._pending: dict[str, _Pending] = {}
        self._request_fragments: dict[str, _RequestFragments] = {}
        self._response_fragments: dict[str, _ResponseFragments] = {}
        self._max_fragment_size = 1 << 20
        self._response_sub: Subscription | None = None
        self._control_subs: list[Subscription] = []
        self._acks_received = 0
        self._function_features: list[FunctionFeature] = []
        self._web_features: list[WebFeature] = []
        self._task_features: list[TaskFeature] = []
        self._workflow_features: list[WorkflowFeature] = []
        self._downstream: list[dict[str, str]] = []
        self._pending_tasks: set[asyncio.Task[Any]] = set()
        self._distrib_cache = DistribCache(self)
        self._trace_runtime: TraceRuntime | None = None

    @property
    def hostname(self) -> str:
        return self._hostname

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def version(self) -> int:
        return self._version

    @property
    def description(self) -> str:
        return self._description

    @property
    def plane(self) -> str:
        return self._plane

    @property
    def locality(self) -> str:
        return self._locality

    @property
    def deployment(self) -> str:
        return self._deployment

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def transport(self) -> Transport:
        return self._transport

    @property
    def distrib_cache(self) -> DistribCache:
        return self._distrib_cache

    def now(self) -> datetime:
        return self._clock()

    def _parse_actor_token(self, token: str) -> Actor:
        verify = self._deployment != "TESTING"
        if not verify:
            return parse_actor(token, verify=False, public_keys=None)
        try:
            iss = token_issuer(token, require_microbus=True)
        except ValueError:
            return parse_actor(token, verify=True, public_keys={})
        keys = dict(self._actor_public_keys.get("*", {}))
        keys.update(self._actor_public_keys.get(iss, {}))
        return parse_actor(token, verify=True, public_keys=keys)

    async def _parse_actor_token_with_jwks(self, token: str) -> Actor:
        try:
            return self._parse_actor_token(token)
        except UnknownActorKeyError:
            if self._deployment == "TESTING":
                raise
            if not self._trusted_issuer_hosts:
                raise ValueError(
                    "jwks refresh is disabled because no trusted_issuer_hosts are configured"
                ) from None
            issuer = token_issuer(token, require_microbus=True)
            issuer_host = _issuer_host(issuer)
            if not issuer_host:
                raise ValueError("actor token issuer is required for jwks refresh") from None
            if issuer_host not in self._trusted_issuer_hosts:
                raise ValueError(
                    f"issuer host {issuer_host!r} is not in trusted_issuer_hosts"
                ) from None
            try:
                await self._refresh_actor_keys(issuer, issuer_host)
                return self._parse_actor_token(token)
            except (TimeoutError, ValueError) as refresh_exc:
                raise ValueError("actor key refresh failed") from refresh_exc

    async def _refresh_actor_keys(self, issuer: str, issuer_host: str) -> None:
        wire = await self.request(
            method="GET",
            url=f"https://{issuer_host}:888/jwks",
            timeout=5.0,
        )
        resp = decode_response(wire)
        if resp.status_code != 200:
            raise ValueError(f"jwks fetch failed with status {resp.status_code}")
        body = json.loads(resp.body)
        if not isinstance(body, Mapping):
            raise ValueError("jwks response must be an object")
        raw_keys = body.get("keys")
        if not isinstance(raw_keys, list):
            raise ValueError("jwks response keys must be a list")
        new_keys: dict[str, Ed25519PublicKey] = {}
        for raw_key in raw_keys:
            if isinstance(raw_key, Mapping):
                kid, key = public_key_from_jwk(raw_key)
                new_keys[kid] = key
        if not new_keys:
            raise ValueError("jwks response has no usable keys")
        existing = self._actor_public_keys.setdefault(issuer, {})
        for kid, key in new_keys.items():
            prior = existing.get(kid)
            if prior is not None:
                prior_bytes = prior.public_bytes_raw()
                new_bytes = key.public_bytes_raw()
                if prior_bytes != new_bytes:
                    raise ValueError(
                        f"kid {kid!r} for issuer {issuer!r} already known with a different key"
                    )
            existing[kid] = key

    def actor_for(self, req: HTTPRequest) -> Actor | None:
        for name, value in req.headers:
            if name == H.ACTOR and value:
                try:
                    return self._parse_actor_token(value)
                except ValueError:
                    return None
        return None

    def current_actor(self) -> Actor | None:
        """Return the verified actor of the in-flight request, or None.

        Available inside `@c.function` and `@c.web` handlers. The framework
        sets the request headers on a context-var before dispatch; this
        method parses the `Microbus-Actor` header against the connector's
        configured public keys. Returns None when no request is in flight,
        no actor header is present, or the token fails verification.
        """
        headers = _CURRENT_REQUEST_HEADERS.get()
        if not headers:
            return None
        for name, value in headers:
            if name == H.ACTOR and value:
                try:
                    return self._parse_actor_token(value)
                except ValueError:
                    return None
        return None

    def current_actor_sub(self) -> str | None:
        """Return the ``sub`` claim of the in-flight actor, or None.

        Non-raising. Returns None when no actor is in flight, the actor
        carries no ``sub`` claim, or ``sub`` is the empty string. For the
        raising variant (used at ownership boundaries) see
        :meth:`require_actor_sub`.
        """
        actor = self.current_actor()
        if actor is None:
            return None
        sub = actor.claims.get("sub")
        if not isinstance(sub, str) or not sub:
            return None
        return sub

    def require_actor_sub(self, *, status: int = 403) -> str:
        """Return the in-flight actor's ``sub`` or raise ``TracedError``.

        Defaults to 403 ``Forbidden`` — the standard answer when the bus
        accepted the request (actor header present and ``required_claims``
        satisfied) but the handler still needs an identified subject for an
        ownership check and the claim is missing or empty. Pass
        ``status=401`` to raise ``Unauthorized`` instead, e.g. when the
        endpoint is unguarded by ``required_claims`` and the missing actor
        is the actual auth failure.
        """
        sub = self.current_actor_sub()
        if sub is None:
            raise TracedError.from_status(status, "forbidden: missing or empty actor sub")
        return sub

    @property
    def acks_received(self) -> int:
        """Cumulative number of ACK frames received on the response subject."""
        return self._acks_received

    def go(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)
        return task

    async def parallel(self, *coros: Coroutine[Any, Any, Any]) -> list[Any]:
        return list(await asyncio.gather(*coros))

    def pending_task_count(self) -> int:
        return len(self._pending_tasks)

    def log_debug(self, msg: str, *args: Any) -> None:
        if not os.environ.get("MICROBUS_LOG_DEBUG"):
            return
        emit_log(sys.stderr, "debug", self.hostname, self.instance_id, msg, args)

    def log_info(self, msg: str, *args: Any) -> None:
        emit_log(sys.stderr, "info", self.hostname, self.instance_id, msg, args)

    def log_warn(self, msg: str, *args: Any) -> None:
        emit_log(sys.stderr, "warning", self.hostname, self.instance_id, msg, args)

    def log_error(self, msg: str, *args: Any) -> None:
        emit_log(sys.stderr, "error", self.hostname, self.instance_id, msg, args)

    def on_startup(self, fn: StartupCallback) -> StartupCallback:
        self._on_startup.append(fn)
        return fn

    def on_shutdown(self, fn: StartupCallback) -> StartupCallback:
        self._on_shutdown.append(fn)
        return fn

    async def startup(self) -> None:
        if self._started:
            raise RuntimeError("connector already started")
        await self._transport.connect("", name=self._hostname)
        try:
            self._trace_runtime = create_trace_runtime(
                hostname=self._hostname,
                version=self._version,
                instance_id=self._instance_id,
                plane=self._plane,
                deployment=self._deployment,
            )
            self._max_fragment_size = self._transport.max_payload() - _FRAGMENT_HEADROOM
            if self._max_fragment_size < _MIN_FRAGMENT_SIZE:
                raise RuntimeError("transport max payload is too small")
            self._response_sub = await self._transport.subscribe(
                subject=response_subject(
                    plane=self._plane,
                    hostname=self._hostname,
                    instance_id=self._instance_id,
                ),
                queue=self._instance_id,
                cb=self._on_response,
            )
            for infra in (*self._control_subscriptions(), *self._infra_subs):
                self._control_subs.extend(await self._bind_subscription(infra))
            for cb in self._on_startup:
                await cb()
            for sub in self._subs:
                sub.sub_handles.extend(await self._bind_subscription(sub))
            await self.start_tickers()
        except _HANDLER_FAILURES:
            await self.stop_tickers()
            await self._unbind_all()
            await self._shutdown_tracer()
            await self._transport.close()
            raise
        self._started = True

    async def shutdown(self) -> None:
        if not self._started:
            return
        await self.stop_tickers()
        await self._unbind_user_subscriptions()
        await self._drain_pending_tasks()
        for cb in self._on_shutdown:
            try:
                await cb()
            except _HANDLER_FAILURES:
                _log.exception("on_shutdown callback raised")
        await self._unbind_infra_subscriptions()
        await self._distrib_cache.close()
        await self._shutdown_tracer()
        await self._transport.close()
        # Release memory pinned to in-flight partial fragments and cancel
        # any pending request futures so awaiters get CancelledError rather
        # than hanging until the connector is GC'd.
        self._request_fragments.clear()
        self._response_fragments.clear()
        for pending in self._pending.values():
            if not pending.ack.done():
                pending.ack.cancel()
            if not pending.response.done():
                pending.response.cancel()
        self._pending.clear()
        self._started = False

    async def _shutdown_tracer(self) -> None:
        if self._trace_runtime is None:
            return
        await asyncio.to_thread(self._trace_runtime.shutdown)
        self._trace_runtime = None

    async def _drain_pending_tasks(self) -> None:
        current = asyncio.current_task()
        tasks = {task for task in self._pending_tasks if task is not current and not task.done()}
        if not tasks:
            return
        done, pending = await asyncio.wait(tasks, timeout=self._shutdown_grace)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            if task.cancelled():
                continue
            try:
                task.result()
            except _HANDLER_FAILURES:
                _log.exception("pending task raised during shutdown")

    async def _unbind_all(self) -> None:
        await self._unbind_user_subscriptions()
        await self._unbind_infra_subscriptions()

    async def _unbind_user_subscriptions(self) -> None:
        for sub in self._subs:
            for handle in sub.sub_handles:
                await handle.unsubscribe()
            sub.sub_handles.clear()

    async def _unbind_infra_subscriptions(self) -> None:
        for handle in self._control_subs:
            await handle.unsubscribe()
        self._control_subs.clear()
        for sub in self._infra_subs:
            sub.sub_handles.clear()
        if self._response_sub is not None:
            await self._response_sub.unsubscribe()
            self._response_sub = None

    async def _bind_subscription(self, sub: _Subscription) -> list[Subscription]:
        host = sub.host_override or self._hostname
        hosts = [host, f"{self._instance_id}.{host}"]
        handles: list[Subscription] = []
        for bound_host in hosts:
            handles.append(
                await self._transport.subscribe(
                    subject=subscription_subject(
                        plane=self._plane,
                        port=str(sub.port),
                        hostname=bound_host,
                        method=sub.method,
                        path=sub.path,
                    ),
                    queue=sub.queue,
                    cb=self._make_dispatcher(sub),
                )
            )
        return handles

    def _drop_expired_fragments(self) -> None:
        for req_key, req_state in list(self._request_fragments.items()):
            if req_state.expired():
                self._request_fragments.pop(req_key, None)
        for resp_key, resp_state in list(self._response_fragments.items()):
            if resp_state.expired():
                self._response_fragments.pop(resp_key, None)

    def _control_subscriptions(self) -> tuple[_Subscription, _Subscription]:
        own = _Subscription(
            name="OpenAPI",
            method="GET",
            path="/openapi.json",
            handler=self._handle_openapi,
            queue=self._hostname,
            required_claims=None,
            description="OpenAPI returns the OpenAPI 3.1 document of the microservice.",
            port=888,
            host_override=None,
        )
        broadcast = _Subscription(
            name="OpenAPIAll",
            method="GET",
            path="/openapi.json",
            handler=self._handle_openapi,
            queue=self._hostname,
            required_claims=None,
            description="OpenAPI returns the OpenAPI 3.1 document of the microservice.",
            port=888,
            host_override="all",
        )
        return own, broadcast

    async def _handle_openapi(self, req: HTTPRequest) -> HTTPResponse:
        claims = await self._claims_from_request(req)
        body = json.dumps(
            build_openapi(self, claims=claims),
            separators=(",", ":"),
        ).encode("utf-8")
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[
                ("Content-Type", "application/json"),
                ("Cache-Control", "private, no-store"),
            ],
            body=body,
        )

    async def _claims_from_request(self, req: HTTPRequest) -> dict[str, Any] | None:
        actor_jwt = Frame(dict(req.headers)).headers.get(H.ACTOR, "")
        if not actor_jwt:
            return None
        try:
            actor = await self._parse_actor_token_with_jwks(actor_jwt)
        except ValueError:
            return None
        return dict(actor.claims)

    def function(
        self, **opts: Any
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
        return function_decorator(self)(**opts)

    def web(self, **opts: Any) -> Callable[[Handler], Handler]:
        return web_decorator(self)(**opts)

    def config(self, **opts: Any) -> Callable[[ConfigGetter], ConfigGetter]:
        return config_decorator(self)(**opts)

    def set_config(self, name: str, value: str) -> None:
        _config_registry(self).set(name, value)

    def get_config(self, name: str) -> str:
        return _config_registry(self).get(name)

    def reset_config(self, name: str) -> None:
        _config_registry(self).reset(name)

    def on_config_changed(self, handler: Callable[[str], None]) -> None:
        _config_registry(self).on_changed(handler)

    def config_printable(self, name: str) -> str:
        return _config_registry(self).printable(name)

    def event(
        self, **opts: Any
    ) -> Callable[[Callable[..., Awaitable[BaseModel]]], EventTrigger[Any, Any]]:
        return event_decorator(self)(**opts)

    def event_sink(self, **opts: Any) -> Callable[[SinkHandler], SinkHandler]:
        return event_sink_decorator(self)(**opts)

    def metric(self, **opts: Any) -> Callable[[MetricRecorder], MetricRecorder]:
        return metric_decorator(self)(**opts)

    def scrape_metrics(self) -> bytes:
        return _metric_registry(self).scrape()

    def task(self, **opts: Any) -> Callable[[TaskHandler], TaskHandler]:
        return task_decorator(self)(**opts)

    def ticker(self, **opts: Any) -> Callable[[TickerHandler], TickerHandler]:
        return ticker_decorator(self)(**opts)

    async def start_tickers(self) -> None:
        await _start_tickers(self)

    async def stop_tickers(self) -> None:
        await _stop_tickers(self)

    def use_clock(self, clock: VirtualClock) -> None:
        _use_clock(self, clock)

    def workflow(self, **opts: Any) -> Callable[[WorkflowFn], WorkflowFn]:
        return workflow_decorator(self)(**opts)

    def register_function_feature(self, feature: FunctionFeature) -> None:
        self._function_features.append(feature)

    def register_web_feature(self, feature: WebFeature) -> None:
        self._web_features.append(feature)

    def register_task_feature(self, feature: TaskFeature) -> None:
        self._task_features.append(feature)

    def register_workflow_feature(self, feature: WorkflowFeature) -> None:
        self._workflow_features.append(feature)

    def list_function_features(self) -> list[FunctionFeature]:
        return list(self._function_features)

    def list_web_features(self) -> list[WebFeature]:
        return list(self._web_features)

    def list_task_features(self) -> list[TaskFeature]:
        return list(self._task_features)

    def list_workflow_features(self) -> list[WorkflowFeature]:
        return list(self._workflow_features)

    def register_downstream(self, *, hostname: str, package: str) -> None:
        entry = {"hostname": hostname, "package": package}
        if entry not in self._downstream:
            self._downstream.append(entry)

    def list_downstream(self) -> list[dict[str, str]]:
        return list(self._downstream)

    def subscribe(
        self,
        *,
        name: str,
        method: str,
        path: str,
        handler: Handler,
        queue: str | None = None,
        required_claims: str | None = None,
        description: str = "",
        port: int | None = None,
    ) -> None:
        eff_port, eff_path, host_override = _parse_route(path)
        if port is not None:
            eff_port = port
        queue_host = host_override or self._hostname
        self._subs.append(
            _Subscription(
                name=name,
                method=method.upper(),
                path=eff_path,
                handler=handler,
                queue=queue_host if queue is None else (queue or None),
                required_claims=required_claims,
                description=description,
                port=eff_port,
                host_override=host_override,
            )
        )

    def _subscribe_infra(
        self,
        *,
        name: str,
        method: str,
        path: str,
        handler: Handler,
        queue: str | None = None,
        port: int | None = None,
    ) -> None:
        eff_port, eff_path, host_override = _parse_route(path)
        if port is not None:
            eff_port = port
        queue_host = host_override or self._hostname
        self._infra_subs.append(
            _Subscription(
                name=name,
                method=method.upper(),
                path=eff_path,
                handler=handler,
                queue=queue_host if queue is None else (queue or None),
                required_claims=None,
                description="Microbus infra subscription.",
                port=eff_port,
                host_override=host_override,
            )
        )

    async def request(
        self,
        *,
        method: str,
        url: str,
        body: bytes = b"",
        headers: list[tuple[str, str]] | None = None,
        actor_token: str | None = None,
        timeout: float = 20.0,
        port: int | None = None,
    ) -> bytes:
        parts = urlsplit(url)
        host = parts.hostname or ""
        eff_port = port or parts.port or (443 if parts.scheme == "https" else 80)
        path = parts.path or "/"

        msg_id = secrets.token_hex(8)
        explicit_headers = list(headers or [])
        overridden_headers = {name.lower() for name, _value in explicit_headers}
        if actor_token:
            overridden_headers.add(H.ACTOR.lower())
        hdrs = [
            *[
                (name, value)
                for name, value in _inbound_propagated_headers()
                if name.lower() not in overridden_headers
            ],
            *explicit_headers,
        ]
        hdrs.append((H.MSG_ID, msg_id))
        hdrs.append((H.FROM_HOST, self._hostname))
        hdrs.append((H.FROM_ID, self._instance_id))
        hdrs.append((H.OP_CODE, OpCode.REQ))
        if actor_token:
            hdrs.append((H.ACTOR, actor_token))

        loop = asyncio.get_event_loop()
        pending = _Pending(ack=loop.create_future(), response=loop.create_future())
        self._pending[msg_id] = pending

        try:
            tracer = self._trace_runtime.tracer if self._trace_runtime else None
            with client_span(
                target_host=host,
                method=method.upper(),
                path=path,
                tracer=tracer,
            ) as _client_span:
                _enrich_client_span_from_body(_client_span, body)
                hdrs = inject_traceparent(hdrs)
                req = HTTPRequest(method=method.upper(), url=url, headers=hdrs, body=body)
                fragments = _fragment_request(req, self._max_fragment_size)
                await self._publish_request_fragment(
                    fragments[0],
                    host=host,
                    method=method,
                    path=path,
                    port=eff_port,
                )
                try:
                    responder_id, _responder_host = await asyncio.wait_for(
                        pending.ack,
                        timeout=self._ack_timeout,
                    )
                except TimeoutError as exc:
                    raise TimeoutError(
                        f"no responders for {method.upper()} {url} (ack timeout)"
                    ) from exc
                for fragment in fragments[1:]:
                    await self._publish_request_fragment(
                        fragment,
                        host=f"{responder_id}.{host}",
                        method=method,
                        path=path,
                        port=eff_port,
                    )
                return await asyncio.wait_for(pending.response, timeout=timeout)
        finally:
            self._pending.pop(msg_id, None)

    async def _publish_request_fragment(
        self,
        req: HTTPRequest,
        *,
        host: str,
        method: str,
        path: str,
        port: int,
    ) -> None:
        await self._transport.publish(
            subject=request_subject(
                plane=self._plane,
                port=str(port),
                hostname=host,
                method=method,
                path=path,
            ),
            payload=encode_request(req),
        )

    async def _on_response(self, msg: IncomingMessage) -> None:
        try:
            resp = decode_response(msg.payload)
        except ValueError:
            _log.warning("dropping malformed response on %r", msg.subject)
            return
        frame = Frame(dict(resp.headers))
        msg_id = frame.msg_id
        pending = self._pending.get(msg_id)
        if pending is None:
            return
        if frame.op_code == OpCode.ACK:
            self._acks_received += 1
            if not pending.ack.done():
                pending.ack.set_result((frame.from_id, frame.from_host))
            return
        resp_payload = msg.payload
        _idx, total = frame.fragment
        if total > 1:
            self._drop_expired_fragments()
            key = f"{frame.from_id}|{msg_id}"
            state = self._response_fragments.setdefault(
                key,
                _ResponseFragments(total=total),
            )
            try:
                integrated = state.add(resp)
            except ValueError:
                _log.warning("dropping malformed response fragment on %r", msg.subject)
                return
            if integrated is None:
                return
            self._response_fragments.pop(key, None)
            resp = integrated
            resp_payload = encode_response(resp)
        if not pending.ack.done():
            pending.ack.set_result((frame.from_id, frame.from_host))
        if not pending.response.done():
            pending.response.set_result(resp_payload)

    def _make_dispatcher(self, sub: _Subscription) -> Callable[[IncomingMessage], Awaitable[None]]:
        async def dispatch(msg: IncomingMessage) -> None:
            current = asyncio.current_task()
            if current is not None:
                self._pending_tasks.add(current)
            try:
                await self._dispatch_message(sub, msg)
            finally:
                if current is not None:
                    self._pending_tasks.discard(current)

        return dispatch

    async def _dispatch_message(self, sub: _Subscription, msg: IncomingMessage) -> None:
        try:
            req = decode_request(msg.payload)
        except ValueError:
            _log.warning("dropping malformed request on %r", msg.subject)
            return

        frame = Frame(dict(req.headers))
        from_host = frame.from_host
        from_id = frame.from_id
        msg_id = frame.msg_id
        if not from_host or not from_id or not msg_id:
            _log.warning("dropping request missing routing headers on %r", msg.subject)
            return

        reply_subject = response_subject(plane=self._plane, hostname=from_host, instance_id=from_id)

        _idx, total = frame.fragment
        await self._send_ack(reply_subject, msg_id, fragmented=total > 1)
        if total > 1:
            self._drop_expired_fragments()
            key = f"{from_id}|{msg_id}"
            state = self._request_fragments.setdefault(
                key,
                _RequestFragments(total=total),
            )
            try:
                integrated = state.add(req)
            except ValueError:
                _log.warning("dropping malformed request fragment on %r", msg.subject)
                return
            if integrated is None:
                return
            self._request_fragments.pop(key, None)
            req = integrated

        tracer = self._trace_runtime.tracer if self._trace_runtime else None
        with server_span(method=req.method, path=sub.path, headers=req.headers, tracer=tracer):
            await self._dispatch_authorized(sub, req, reply_subject, msg_id)

    async def _dispatch_authorized(
        self,
        sub: _Subscription,
        req: HTTPRequest,
        reply_subject: str,
        msg_id: str,
    ) -> None:
        frame = Frame(dict(req.headers))
        if sub.required_claims:
            actor_jwt = frame.headers.get(H.ACTOR, "")
            if not actor_jwt:
                await self._send_error(reply_subject, msg_id, status=401, message="missing actor")
                return
            try:
                actor = await self._parse_actor_token_with_jwks(actor_jwt)
            except ValueError:
                await self._send_error(reply_subject, msg_id, status=401, message="invalid actor")
                return
            expr = compile_expr(sub.required_claims)
            if not expr.evaluate(actor.claims):
                await self._send_error(
                    reply_subject,
                    msg_id,
                    status=403,
                    message="required claims not satisfied",
                )
                return

        try:
            token = _CURRENT_REQUEST_HEADERS.set(req.headers)
            try:
                resp = await sub.handler(req)
            finally:
                _CURRENT_REQUEST_HEADERS.reset(token)
        except TracedError as exc:
            resp = HTTPResponse(
                status_code=exc.status_code,
                reason=HTTP_REASONS.get(exc.status_code, "Error"),
                headers=[("Content-Type", "application/json")],
                body=json_error_body(exc),
            )
        except _HANDLER_FAILURES as exc:
            err = TracedError("handler error", status_code=500, cause=exc)
            resp = HTTPResponse(
                status_code=500,
                reason=HTTP_REASONS[500],
                headers=[("Content-Type", "application/json")],
                body=json_error_body(err),
            )

        resp.headers = [
            *[(name, value) for name, value in resp.headers if name not in _RESPONSE_FRAME_HEADERS],
            (H.MSG_ID, msg_id),
            (H.FROM_HOST, self._hostname),
            (H.FROM_ID, self._instance_id),
            (H.OP_CODE, OpCode.RES),
        ]
        for fragment in _fragment_response(resp, self._max_fragment_size):
            await self._transport.publish(
                subject=reply_subject,
                payload=encode_response(fragment),
            )

    async def _send_ack(self, reply_subject: str, msg_id: str, *, fragmented: bool) -> None:
        status = 100 if fragmented else 202
        ack = HTTPResponse(
            status_code=status,
            reason=HTTP_REASONS.get(status, ""),
            headers=[
                (H.MSG_ID, msg_id),
                (H.FROM_HOST, self._hostname),
                (H.FROM_ID, self._instance_id),
                (H.OP_CODE, OpCode.ACK),
            ],
            body=b"",
        )
        await self._transport.publish(subject=reply_subject, payload=encode_response(ack))

    async def _send_error(
        self,
        reply_subject: str,
        msg_id: str,
        *,
        status: int,
        message: str,
    ) -> None:
        resp = HTTPResponse(
            status_code=status,
            reason=HTTP_REASONS.get(status, "Error"),
            headers=[
                ("Content-Type", "application/json"),
                (H.OP_CODE, OpCode.ERR),
                (H.FROM_HOST, self._hostname),
                (H.FROM_ID, self._instance_id),
                (H.MSG_ID, msg_id),
            ],
            body=json_error_body(TracedError(message, status_code=status)),
        )
        await self._transport.publish(subject=reply_subject, payload=encode_response(resp))
