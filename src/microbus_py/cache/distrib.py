"""Connector-owned distributed cache.

fabric@v1.27.1 creates a distributed LRU cache for every connector at
``:888/dcache``. The cache is not a central service: each replica owns a local
shard and coordinates through Microbus requests to the same service hostname.
This module implements the Python side of that peer protocol.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import secrets
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, quote, urlsplit

from microbus_py.frame.frame import Frame
from microbus_py.frame.headers import H, OpCode
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response, encode_request
from microbus_py.wire.subjects import request_subject, response_subject
from microbus_py.workflow.flow import format_go_duration

if TYPE_CHECKING:
    from collections.abc import Callable

    from microbus_py.connector.connector import Connector
    from microbus_py.transport.base import IncomingMessage

__all__ = ["DistribCache"]

_CACHE_PORT = 888
_CACHE_PATH = "/dcache"
_ACK_WINDOW_SECONDS = 0.05
_COLLECT_WINDOW_SECONDS = 0.05
_DEFAULT_MAX_AGE = timedelta(hours=1)


@dataclass(slots=True)
class _CollectState:
    expected: int = 0
    completed: int = 0
    ack_phase: bool = True


class DistribCache:
    """Distributed cache attached to a connector instance."""

    def __init__(
        self,
        svc: Connector,
        *,
        path: str = _CACHE_PATH,
        max_age: timedelta = _DEFAULT_MAX_AGE,
    ) -> None:
        self._svc = svc
        self._path = path.rstrip("/")
        self._base_url = f"https://{svc.hostname}:{_CACHE_PORT}{self._path}"
        self._max_age = max_age
        self._store: dict[str, bytes] = {}
        svc._subscribe_infra(
            name="DcacheAll",
            method="ANY",
            path=f":{_CACHE_PORT}{self._path}/all",
            handler=self._handle_all,
            queue="",
        )
        svc._subscribe_infra(
            name="DcacheRescue",
            method="ANY",
            path=f":{_CACHE_PORT}{self._path}/rescue",
            handler=self._handle_rescue,
        )

    def local_len(self) -> int:
        return len(self._store)

    def local_weight(self) -> int:
        return sum(len(value) for value in self._store.values())

    async def store(self, key: str, value: bytes, *, replicate: bool = False) -> None:
        self._check_key(key)
        self._store.pop(key, None)
        if replicate:
            await self._publish_all("PUT", {"do": "store", "key": key}, body=value)
        else:
            await self._publish_all("DELETE", {"do": "delete", "key": key})
        self._store[key] = value

    async def set(self, key: str, value: bytes, ttl: float | None = None) -> None:
        _ = ttl
        await self.store(key, value)

    async def load(
        self,
        key: str,
        *,
        bump: bool = True,
        consistency_check: bool = True,
        max_age: timedelta | None = None,
    ) -> bytes | None:
        self._check_key(key)
        local = self._store.get(key)
        if local is not None:
            if consistency_check and not await self._local_consistent(key, local):
                self._store.pop(key, None)
                return None
            return local

        ttl = format_go_duration(max_age or self._max_age)
        responses = await self._publish_all(
            "GET",
            {"do": "load", "bump": str(bump).lower(), "key": key, "ttl": ttl},
        )
        found: bytes | None = None
        for resp in responses:
            if resp.status_code != 200:
                continue
            if consistency_check and found is not None and found != resp.body:
                return None
            found = resp.body
        return found

    async def get(self, key: str) -> bytes | None:
        return await self.load(key)

    async def peek(self, key: str) -> bytes | None:
        return await self.load(key, bump=False)

    async def has(self, key: str) -> bool:
        return await self.load(key, bump=False) is not None

    async def delete(self, key: str) -> None:
        self._check_key(key)
        self._store.pop(key, None)
        await self._publish_all("DELETE", {"do": "delete", "key": key})

    async def delete_prefix(self, prefix: str) -> None:
        if not prefix:
            raise ValueError("missing prefix")
        self._delete_local(lambda key: key.startswith(prefix))
        await self._publish_all("DELETE", {"do": "delete", "prefix": prefix})

    async def delete_contains(self, substring: str) -> None:
        if not substring:
            raise ValueError("missing substring")
        self._delete_local(lambda key: substring in key)
        await self._publish_all("DELETE", {"do": "delete", "contains": substring})

    async def clear(self) -> None:
        await self._publish_all("DELETE", {"do": "clear"})

    async def close(self) -> None:
        await self._offload()
        self._store.clear()

    async def len(self) -> int:
        total = 0
        for resp in await self._publish_all("GET", {"do": "len"}):
            if resp.status_code == 200:
                total += int(resp.body.decode("ascii") or "0")
        return total

    async def weight(self) -> int:
        total = 0
        for resp in await self._publish_all("GET", {"do": "weight"}):
            if resp.status_code == 200:
                total += int(resp.body.decode("ascii") or "0")
        return total

    async def _local_consistent(self, key: str, value: bytes) -> bool:
        checksum = hashlib.sha256(value).hexdigest()
        responses = await self._publish_all(
            "GET",
            {"do": "checksum", "key": key, "checksum": checksum},
        )
        for resp in responses:
            if resp.status_code == 200 and resp.body != bytes.fromhex(checksum):
                return False
        return True

    async def _publish_all(
        self,
        method: str,
        query: dict[str, str],
        *,
        body: bytes = b"",
    ) -> list[HTTPResponse]:
        return await self._publish_collect(method, f"{self._base_url}/all{_query(query)}", body)

    async def _publish_collect(self, method: str, url: str, body: bytes) -> list[HTTPResponse]:
        msg_id = secrets.token_hex(8)
        subject = request_subject(
            plane=self._svc.plane,
            port=str(_CACHE_PORT),
            hostname=self._svc.hostname,
            method=method,
            path=urlsplit(url).path,
        )
        reply_subject = response_subject(
            plane=self._svc.plane,
            hostname=self._svc.hostname,
            instance_id=self._svc.instance_id,
        )
        req = HTTPRequest(
            method=method,
            url=url,
            headers=[
                (H.MSG_ID, msg_id),
                (H.FROM_HOST, self._svc.hostname),
                (H.FROM_ID, self._svc.instance_id),
                (H.OP_CODE, OpCode.REQ),
            ],
            body=body,
        )
        responses: list[HTTPResponse] = []
        seen_acks: set[str] = set()
        state = _CollectState()
        done = asyncio.Event()

        async def on_response(msg: IncomingMessage) -> None:
            try:
                resp = decode_response(msg.payload)
            except ValueError:
                return
            frame = Frame(dict(resp.headers))
            if frame.msg_id != msg_id:
                return
            responder = frame.from_id or frame.from_host
            if frame.op_code == OpCode.ACK:
                if responder and responder not in seen_acks:
                    seen_acks.add(responder)
                    state.expected += 1
                return
            responses.append(resp)
            state.completed += 1
            if not state.ack_phase and state.expected > 0 and state.completed >= state.expected:
                done.set()

        sub = await self._svc.transport.subscribe(subject=reply_subject, queue=None, cb=on_response)
        try:
            await self._svc.transport.publish(subject=subject, payload=encode_request(req))
            await asyncio.sleep(_ACK_WINDOW_SECONDS)
            state.ack_phase = False
            if state.expected == 0:
                return []
            if state.completed >= state.expected:
                done.set()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(done.wait(), timeout=_COLLECT_WINDOW_SECONDS)
            return list(responses)
        finally:
            await sub.unsubscribe()

    async def _offload(self) -> None:
        for key, value in list(self._store.items()):
            responses = await self._publish_collect(
                "PUT",
                f"{self._base_url}/rescue{_query({'key': key})}",
                value,
            )
            if not responses:
                return

    async def _handle_all(self, req: HTTPRequest) -> HTTPResponse:
        query = _query_dict(req.url)
        handlers = {
            "load": self._handle_load,
            "store": self._handle_store,
            "checksum": self._handle_checksum,
            "delete": self._handle_delete,
            "clear": self._handle_clear,
            "weight": self._handle_weight,
            "len": self._handle_len,
        }
        handler = handlers.get(query.get("do", ""))
        if handler is None:
            return _response(400, b"invalid cache action")
        return handler(req, query)

    async def _handle_rescue(self, req: HTTPRequest) -> HTTPResponse:
        frame = Frame(dict(req.headers))
        if frame.from_id == self._svc.instance_id:
            return _response(200, b"")
        key = _query_dict(req.url).get("key", "")
        if not key:
            return _response(400, b"missing key")
        self._store[key] = req.body
        return _response(200, b"")

    def _handle_load(self, req: HTTPRequest, query: dict[str, str]) -> HTTPResponse:
        frame = Frame(dict(req.headers))
        if frame.from_id == self._svc.instance_id:
            return _response(404, b"")
        key = query.get("key", "")
        if not key:
            return _response(400, b"missing key")
        data = self._store.get(key)
        if data is None:
            return _response(404, b"")
        return _response(200, data)

    def _handle_store(self, req: HTTPRequest, query: dict[str, str]) -> HTTPResponse:
        frame = Frame(dict(req.headers))
        if frame.from_id == self._svc.instance_id:
            return _response(404, b"")
        key = query.get("key", "")
        if not key:
            return _response(400, b"missing key")
        self._store[key] = req.body
        return _response(200, b"")

    def _handle_checksum(self, req: HTTPRequest, query: dict[str, str]) -> HTTPResponse:
        frame = Frame(dict(req.headers))
        if frame.from_id == self._svc.instance_id:
            return _response(404, b"")
        key = query.get("key", "")
        checksum = query.get("checksum", "")
        if not key or not checksum:
            return _response(400, b"missing checksum")
        data = self._store.get(key)
        if data is None:
            return _response(404, b"")
        local = hashlib.sha256(data).digest()
        if local.hex() != checksum:
            self._store.pop(key, None)
        return _response(200, local)

    def _handle_delete(self, req: HTTPRequest, query: dict[str, str]) -> HTTPResponse:
        frame = Frame(dict(req.headers))
        if frame.from_id == self._svc.instance_id:
            return _response(200, b"")
        key = query.get("key", "")
        if key:
            self._store.pop(key, None)
            return _response(200, b"")
        prefix = query.get("prefix", "")
        if prefix:
            self._delete_local(lambda local_key: local_key.startswith(prefix))
            return _response(200, b"")
        contains = query.get("contains", "")
        if contains:
            self._delete_local(lambda local_key: contains in local_key)
            return _response(200, b"")
        return _response(400, b"missing key")

    def _delete_local(self, predicate: Callable[[str], bool]) -> None:
        for key in [key for key in self._store if predicate(key)]:
            self._store.pop(key, None)

    def _handle_clear(self, _req: HTTPRequest, _query: dict[str, str]) -> HTTPResponse:
        self._store.clear()
        return _response(200, b"")

    def _handle_weight(self, _req: HTTPRequest, _query: dict[str, str]) -> HTTPResponse:
        return _response(200, str(self.local_weight()).encode("ascii"))

    def _handle_len(self, _req: HTTPRequest, _query: dict[str, str]) -> HTTPResponse:
        return _response(200, str(self.local_len()).encode("ascii"))

    @staticmethod
    def _check_key(key: str) -> None:
        if not key:
            raise ValueError("missing key")


def _query_dict(url: str) -> dict[str, str]:
    parsed = parse_qs(urlsplit(url).query)
    return {key: values[0] for key, values in parsed.items() if values}


def _query(values: dict[str, str]) -> str:
    pairs = [f"{quote(key, safe='')}={quote(value, safe='')}" for key, value in values.items()]
    return "?" + "&".join(pairs)


def _response(status: int, body: bytes) -> HTTPResponse:
    reasons = {200: "OK", 400: "Bad Request", 404: "Not Found"}
    return HTTPResponse(status_code=status, reason=reasons[status], headers=[], body=body)
