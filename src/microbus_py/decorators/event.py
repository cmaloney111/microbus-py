"""``@event`` decorator — declare an outbound event with a multicast trigger.

The decorator returns a callable trigger object that publishes the event
to all subscribers on call and yields a typed response per subscriber. A
``trigger.fire(payload)`` shortcut performs fire-and-forget publication.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import secrets
import typing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from pydantic import BaseModel

from microbus_py.connector.route import parse_route
from microbus_py.frame.frame import Frame
from microbus_py.frame.headers import H, OpCode
from microbus_py.wire.codec import HTTPRequest, decode_response, encode_request
from microbus_py.wire.subjects import request_subject, response_subject

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from microbus_py.connector.connector import Connector
    from microbus_py.transport.base import IncomingMessage

__all__ = ["EventFeature", "EventTrigger", "event_decorator"]


_DEFAULT_EVENT_PORT = 417
_DEFAULT_COLLECT_AFTER_LAST = 0.05


@dataclass(slots=True, frozen=True)
class EventFeature:
    name: str
    route: str
    method: str
    port: int
    input_model: type[BaseModel]
    output_model: type[BaseModel]


T = TypeVar("T", bound=BaseModel)
R = TypeVar("R", bound=BaseModel)


class EventTrigger(Generic[T, R]):
    """Publishes the event and exposes responses as an async iterator."""

    __slots__ = ("ack_window", "collect_window", "feature", "svc")

    def __init__(
        self,
        svc: Connector,
        feature: EventFeature,
        ack_window: float = 0.1,
        collect_window: float = _DEFAULT_COLLECT_AFTER_LAST,
    ) -> None:
        self.svc = svc
        self.feature = feature
        self.ack_window = ack_window
        self.collect_window = collect_window

    def __call__(self, payload: T) -> AsyncIterator[R]:
        return cast(
            "AsyncIterator[R]",
            _publish_and_collect(
                svc=self.svc,
                feature=self.feature,
                payload=payload,
                ack_window=self.ack_window,
                collect_window=self.collect_window,
            ),
        )

    async def fire(self, payload: T) -> None:
        await _publish(svc=self.svc, feature=self.feature, payload=payload, msg_id=None)


def event_decorator(
    svc: Connector,
) -> Callable[..., Callable[[Callable[..., Awaitable[BaseModel]]], EventTrigger[Any, Any]]]:
    def deco(
        *,
        route: str,
        method: str = "POST",
    ) -> Callable[[Callable[..., Awaitable[BaseModel]]], EventTrigger[Any, Any]]:
        port, path, _ = parse_route(route)
        if port == 443:
            port = _DEFAULT_EVENT_PORT

        def wrap(handler: Callable[..., Awaitable[BaseModel]]) -> EventTrigger[Any, Any]:
            input_model, output_model = _resolve_models(handler)
            feature = EventFeature(
                name=handler.__name__,
                route=path,
                method=method.upper(),
                port=port,
                input_model=input_model,
                output_model=output_model,
            )
            features = svc.__dict__.setdefault("_event_features", [])
            features.append(feature)
            return EventTrigger(svc, feature)

        return wrap

    return deco


def _resolve_models(
    handler: Callable[..., Awaitable[BaseModel]],
) -> tuple[type[BaseModel], type[BaseModel]]:
    hints = typing.get_type_hints(handler)
    sig = inspect.signature(handler)
    params = [p for p in sig.parameters.values() if p.kind != inspect.Parameter.VAR_KEYWORD]
    if not params:
        raise TypeError(f"@event handler {handler.__name__} requires one Pydantic input parameter")
    input_type = hints.get(params[0].name)
    if not (isinstance(input_type, type) and issubclass(input_type, BaseModel)):
        raise TypeError(
            f"@event handler {handler.__name__} input must be a Pydantic BaseModel subclass"
        )
    return_type = hints.get("return")
    if not (isinstance(return_type, type) and issubclass(return_type, BaseModel)):
        raise TypeError(
            f"@event handler {handler.__name__} must annotate a Pydantic BaseModel return type"
        )
    return input_type, return_type


async def _publish(
    *,
    svc: Connector,
    feature: EventFeature,
    payload: BaseModel,
    msg_id: str | None,
) -> str:
    body = payload.model_dump_json().encode("utf-8")
    used_msg_id = msg_id or secrets.token_hex(8)
    url = f"https://{svc.hostname}:{feature.port}{feature.route}"
    headers: list[tuple[str, str]] = [
        ("Content-Type", "application/json"),
        (H.MSG_ID, used_msg_id),
        (H.FROM_HOST, svc.hostname),
        (H.FROM_ID, svc.instance_id),
        (H.OP_CODE, OpCode.REQ),
    ]
    req = HTTPRequest(method=feature.method, url=url, headers=headers, body=body)
    wire = encode_request(req)
    subject = request_subject(
        plane=svc.plane,
        port=str(feature.port),
        hostname=svc.hostname,
        method=feature.method,
        path=feature.route,
    )
    await svc.transport.publish(subject=subject, payload=wire)
    return used_msg_id


async def _publish_and_collect(
    *,
    svc: Connector,
    feature: EventFeature,
    payload: BaseModel,
    ack_window: float,
    collect_window: float,
) -> AsyncIterator[BaseModel]:
    inbox_id = secrets.token_hex(8)
    msg_id = secrets.token_hex(8)
    body = payload.model_dump_json().encode("utf-8")
    url = f"https://{svc.hostname}:{feature.port}{feature.route}"
    headers: list[tuple[str, str]] = [
        ("Content-Type", "application/json"),
        (H.MSG_ID, msg_id),
        (H.FROM_HOST, svc.hostname),
        (H.FROM_ID, inbox_id),
        (H.OP_CODE, OpCode.REQ),
    ]
    req = HTTPRequest(method=feature.method, url=url, headers=headers, body=body)
    wire = encode_request(req)
    inbox_subject = response_subject(plane=svc.plane, hostname=svc.hostname, instance_id=inbox_id)
    request_subj = request_subject(
        plane=svc.plane,
        port=str(feature.port),
        hostname=svc.hostname,
        method=feature.method,
        path=feature.route,
    )

    queue: asyncio.Queue[BaseModel | None] = asyncio.Queue()
    state = {"expected": 0, "ack_phase": True, "completed": 0}
    seen_responders: set[str] = set()
    done_event = asyncio.Event()

    async def on_response(msg: IncomingMessage) -> None:
        try:
            resp = decode_response(msg.payload)
        except ValueError:
            return
        frame = Frame(dict(resp.headers))
        if frame.msg_id != msg_id:
            return
        responder = frame.from_id or frame.from_host or ""
        if frame.op_code == OpCode.ACK:
            if responder and responder not in seen_responders:
                seen_responders.add(responder)
                state["expected"] += 1
            return
        try:
            obj = feature.output_model.model_validate_json(resp.body)
        except ValueError:
            obj = None
        if obj is not None:
            await queue.put(obj)
        state["completed"] += 1
        if (
            not state["ack_phase"]
            and state["expected"] > 0
            and state["completed"] >= state["expected"]
        ):
            done_event.set()

    sub = await svc.transport.subscribe(subject=inbox_subject, queue=None, cb=on_response)
    try:
        await svc.transport.publish(subject=request_subj, payload=wire)
        await asyncio.sleep(ack_window)
        state["ack_phase"] = False
        if state["expected"] == 0:
            return
        if state["completed"] >= state["expected"]:
            done_event.set()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(done_event.wait(), timeout=5.0)
        await asyncio.sleep(collect_window)
        await queue.put(None)
        while True:
            item = await queue.get()
            if item is None:
                return
            yield item
    finally:
        await sub.unsubscribe()
