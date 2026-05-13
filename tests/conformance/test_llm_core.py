"""LLM core conformance with a Python provider."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel, Field

from microbus_py.wire.codec import decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


class Message(BaseModel):
    role: str = ""
    content: str = ""
    toolCallId: str = ""
    toolCalls: str = ""


class Tool(BaseModel):
    name: str = ""
    description: str = ""
    inputSchema: dict[str, object] | None = None
    url: str = ""
    method: str = ""
    type: str = ""


class ToolCall(BaseModel):
    id: str = ""
    name: str = ""
    arguments: dict[str, object] | None = None


class TurnCompletion(BaseModel):
    content: str = ""
    toolCalls: list[ToolCall] = Field(default_factory=list)


class TurnIn(BaseModel):
    messages: list[Message] = Field(default_factory=list)
    tools: list[Tool] = Field(default_factory=list)


class TurnOut(BaseModel):
    completion: TurnCompletion


class SquareIn(BaseModel):
    x: int


class SquareOut(BaseModel):
    y: int


async def test_go_llm_core_chat_delegates_to_python_provider(
    conformance_env: ConformanceEnv,
) -> None:
    provider = await connector_for(conformance_env, "py-llm.example")
    caller = await connector_for(conformance_env, "py-llm-client.example")

    @provider.function(route=":444/turn", method="POST", name="Turn")
    async def turn(inp: TurnIn) -> TurnOut:
        prompt = inp.messages[-1].content if inp.messages else ""
        return TurnOut(completion=TurnCompletion(content=f"python provider saw: {prompt}"))

    await provider.startup()
    await caller.startup()
    try:
        resp_bytes = await caller.request(
            method="POST",
            url="https://llm.core:444/chat",
            body=json.dumps(
                {"messages": [{"role": "user", "content": "hello from conformance"}]}
            ).encode("utf-8"),
            headers=[("Content-Type", "application/json")],
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        payload = json.loads(resp.body)
        assert payload == {
            "messagesOut": [
                {"role": "assistant", "content": "python provider saw: hello from conformance"}
            ]
        }
    finally:
        await caller.shutdown()
        await provider.shutdown()


async def test_go_llm_core_chat_forwards_python_tool_schema_to_python_provider(
    conformance_env: ConformanceEnv,
) -> None:
    provider = await connector_for(conformance_env, "py-llm.example")
    tool_service = await connector_for(conformance_env, "py-llm-tool.example")
    caller = await connector_for(conformance_env, "py-llm-tool-client.example")
    seen_tools: list[Tool] = []

    @provider.function(route=":444/turn", method="POST", name="Turn")
    async def turn(inp: TurnIn) -> TurnOut:
        seen_tools[:] = inp.tools
        tool = inp.tools[0]
        return TurnOut(
            completion=TurnCompletion(
                content=f"tool:{tool.name}:{tool.method}:{tool.type}:{tool.url}"
            )
        )

    @tool_service.function(
        route="/square",
        method="POST",
        name="Square",
        description="Square an integer",
    )
    async def square(inp: SquareIn) -> SquareOut:
        return SquareOut(y=inp.x * inp.x)

    await provider.startup()
    await tool_service.startup()
    await caller.startup()
    try:
        tool_url = "https://py-llm-tool.example:443/square"
        resp_bytes = await caller.request(
            method="POST",
            url="https://llm.core:444/chat",
            body=json.dumps(
                {
                    "messages": [{"role": "user", "content": "which tool is available?"}],
                    "tools": [tool_url],
                }
            ).encode("utf-8"),
            headers=[("Content-Type", "application/json")],
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        payload = json.loads(resp.body)
        assert payload == {
            "messagesOut": [
                {
                    "role": "assistant",
                    "content": f"tool:Square:POST:function:{tool_url}",
                }
            ]
        }
        assert len(seen_tools) == 1
        assert seen_tools[0].inputSchema is not None
        assert seen_tools[0].inputSchema["type"] == "object"
        assert seen_tools[0].inputSchema["required"] == ["x"]
    finally:
        await caller.shutdown()
        await tool_service.shutdown()
        await provider.shutdown()
