# microbus-py

A try at getting [Microbus](https://github.com/microbus-io/fabric) to work from Python. The wire is HTTP-over-NATS, so a Python service can join the same bus as Go peers and look the same on the wire.

Work in progress. Pre-alpha. Targets fabric `v1.27.1`.

## Install

```bash
uv add microbus-py
```

## Hello world

```python
from pydantic import BaseModel
from microbus_py import Connector


class GreetIn(BaseModel):
    name: str


class GreetOut(BaseModel):
    message: str


svc = Connector(hostname="hello.example")


@svc.function(route="/greet", method="POST")
async def greet(inp: GreetIn) -> GreetOut:
    return GreetOut(message=f"Hello {inp.name}")
```

## Two services on one bus

```python
import asyncio
import json

from pydantic import BaseModel

from microbus_py import Connector, InMemoryBroker
from microbus_py.wire.codec import decode_response


class UpperIn(BaseModel):
    text: str


class UpperOut(BaseModel):
    text: str


async def main() -> None:
    bus = InMemoryBroker()
    worker = Connector(hostname="upper.example", transport=bus, deployment="LOCAL")
    caller = Connector(hostname="caller.example", transport=bus, deployment="LOCAL")

    @worker.function(route="/upper", method="POST")
    async def to_upper(inp: UpperIn) -> UpperOut:
        return UpperOut(text=inp.text.upper())

    await worker.startup()
    await caller.startup()
    try:
        wire = await caller.request(
            method="POST",
            url="https://upper.example:443/upper",
            body=json.dumps({"text": "hello"}).encode(),
            headers=[("Content-Type", "application/json")],
            timeout=2.0,
        )
        resp = decode_response(wire)
        print(resp.status_code, resp.body.decode())
    finally:
        await caller.shutdown()
        await worker.shutdown()


asyncio.run(main())
```

## Background heartbeat

```python
import asyncio
from datetime import timedelta

from microbus_py import Connector, InMemoryBroker


async def main() -> None:
    svc = Connector(
        hostname="heartbeat.example",
        transport=InMemoryBroker(),
        deployment="LOCAL",
    )

    fired = 0

    @svc.ticker(name="beat", interval=timedelta(milliseconds=200))
    async def beat() -> None:
        nonlocal fired
        fired += 1
        print(f"tick {fired}")

    await svc.start_tickers()
    await asyncio.sleep(0.7)
    await svc.stop_tickers()


asyncio.run(main())
```

## Run tests

```bash
uv run pytest tests/unit          # fast, no infra
./scripts/integration.sh          # needs NATS (script will start it via Docker if not running)
./scripts/conformance.sh          # needs Docker + Go (script builds the sidecar from docker/gosidecar/)
```

If you want NATS running yourself: `docker run --rm -p 4222:4222 nats:2.10`. The conformance script builds the Go fabric sidecar locally, so no manual setup beyond Docker and Go.
