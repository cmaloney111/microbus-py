"""Boolean expression engine for ``RequiredClaims``.

Mirrors the grammar in fabric@v1.27.1/frame/ifactor.go::

    expr   ::= or_expr
    or_expr  ::= and_expr ("||" and_expr)*
    and_expr ::= not_expr ("&&" not_expr)*
    not_expr ::= "!" not_expr | atom
    atom     ::= comparison | "(" expr ")" | identifier
    comparison ::= path op value
    op       ::= "==" | "!=" | "=~" | "!~" | ">" | "<" | ">=" | "<="
    path     ::= IDENT ("." IDENT)*
    value    ::= STRING | NUMBER | path

Arrays in the input context are treated as sets — equality against a
scalar matches when the value is a member. Dot navigation into nested
mappings is supported; missing keys yield ``None`` (and therefore false
for any comparison except ``!=``).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Protocol

__all__ = ["ClaimsExpr", "compile_expr"]


# ---------------------------------------------------------------------------
# Lexer
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Tok:
    kind: str
    value: str


_TOKEN_RE = re.compile(
    r"""
    \s+                              |   # whitespace
    (?P<STRING>"(?:\\.|[^"\\])*")    |   # double-quoted string
    (?P<SQUOTE>'(?:\\.|[^'\\])*')    |   # single-quoted string
    (?P<NUMBER>-?\d+(?:\.\d+)?)      |   # numeric literal
    (?P<EQ>==)                       |
    (?P<NEQ>!=)                      |
    (?P<REQ>=~)                      |
    (?P<NREQ>!~)                     |
    (?P<GE>>=)                       |
    (?P<LE><=)                       |
    (?P<AND>&&)                      |
    (?P<OR>\|\|)                     |
    (?P<GT>>)                        |
    (?P<LT><)                        |
    (?P<NOT>!)                       |
    (?P<LPAREN>\()                   |
    (?P<RPAREN>\))                   |
    (?P<DOT>\.)                      |
    (?P<IDENT>[A-Za-z_][A-Za-z0-9_]*)
    """,
    re.VERBOSE,
)


def _lex(src: str) -> list[_Tok]:
    tokens: list[_Tok] = []
    pos = 0
    while pos < len(src):
        m = _TOKEN_RE.match(src, pos)
        if not m:
            raise ValueError(f"unexpected character at offset {pos}: {src[pos]!r}")
        if m.lastgroup is None:
            pos = m.end()
            continue
        kind = m.lastgroup
        value = m.group()
        if kind in {"STRING", "SQUOTE"}:
            value = value[1:-1]
            kind = "STRING"
        tokens.append(_Tok(kind=kind, value=value))
        pos = m.end()
    return tokens


# ---------------------------------------------------------------------------
# AST nodes
# ---------------------------------------------------------------------------


class _Node(Protocol):
    def evaluate(self, ctx: Mapping[str, Any]) -> bool: ...


@dataclass(slots=True)
class _Or:
    left: _Node
    right: _Node

    def evaluate(self, ctx: Mapping[str, Any]) -> bool:
        return self.left.evaluate(ctx) or self.right.evaluate(ctx)


@dataclass(slots=True)
class _And:
    left: _Node
    right: _Node

    def evaluate(self, ctx: Mapping[str, Any]) -> bool:
        return self.left.evaluate(ctx) and self.right.evaluate(ctx)


@dataclass(slots=True)
class _Not:
    inner: _Node

    def evaluate(self, ctx: Mapping[str, Any]) -> bool:
        return not self.inner.evaluate(ctx)


@dataclass(slots=True)
class _Compare:
    path: tuple[str, ...]
    op: str
    rhs: Any

    def evaluate(self, ctx: Mapping[str, Any]) -> bool:
        lhs = _resolve(ctx, self.path)
        rhs = self.rhs
        if self.op in {"==", "!="}:
            eq = rhs in lhs if isinstance(lhs, list) else lhs == rhs
            return eq if self.op == "==" else not eq
        if self.op in {"=~", "!~"}:
            if not isinstance(lhs, str) or not isinstance(rhs, str):
                return self.op != "=~"
            matched = re.search(rhs, lhs) is not None
            return matched if self.op == "=~" else not matched
        return _COMPARATORS[self.op](lhs, rhs)


_COMPARATORS: dict[str, Callable[[Any, Any], bool]] = {
    ">": lambda a, b: a is not None and b is not None and a > b,
    "<": lambda a, b: a is not None and b is not None and a < b,
    ">=": lambda a, b: a is not None and b is not None and a >= b,
    "<=": lambda a, b: a is not None and b is not None and a <= b,
}


@dataclass(slots=True)
class _Truthy:
    """Bare path or path-into-array — true when present and truthy."""

    path: tuple[str, ...]

    def evaluate(self, ctx: Mapping[str, Any]) -> bool:
        # If the second-to-last segment resolves to a list, treat the last
        # segment as set-membership (e.g. ``roles.admin``).
        if len(self.path) >= 2:
            head = _resolve(ctx, self.path[:-1])
            if isinstance(head, list):
                return self.path[-1] in head
        value = _resolve(ctx, self.path)
        return bool(value)


def _resolve(ctx: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    cur: Any = ctx
    for seg in path:
        if isinstance(cur, Mapping) and seg in cur:
            cur = cur[seg]
        else:
            return None
    return cur


# ---------------------------------------------------------------------------
# Parser (recursive descent)
# ---------------------------------------------------------------------------


class _Parser:
    def __init__(self, tokens: list[_Tok]) -> None:
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> _Tok | None:
        return self._tokens[self._pos] if self._pos < len(self._tokens) else None

    def _consume(self, kind: str | None = None) -> _Tok:
        tok = self._peek()
        if tok is None:
            raise ValueError("unexpected end of expression")
        if kind is not None and tok.kind != kind:
            raise ValueError(f"expected {kind}, got {tok.kind}")
        self._pos += 1
        return tok

    def parse(self) -> _Node:
        node = self._or()
        if self._pos < len(self._tokens):
            extra = self._tokens[self._pos]
            raise ValueError(f"unexpected token {extra.kind} {extra.value!r}")
        return node

    def _or(self) -> _Node:
        node = self._and()
        while (tok := self._peek()) is not None and tok.kind == "OR":
            self._consume("OR")
            node = _Or(node, self._and())
        return node

    def _and(self) -> _Node:
        node = self._not()
        while (tok := self._peek()) is not None and tok.kind == "AND":
            self._consume("AND")
            node = _And(node, self._not())
        return node

    def _not(self) -> _Node:
        tok = self._peek()
        if tok is not None and tok.kind == "NOT":
            self._consume("NOT")
            return _Not(self._not())
        return self._atom()

    def _atom(self) -> _Node:
        tok = self._peek()
        if tok is None:
            raise ValueError("unexpected end of expression")
        if tok.kind == "LPAREN":
            self._consume("LPAREN")
            node = self._or()
            self._consume("RPAREN")
            return node
        if tok.kind == "IDENT":
            path = self._path()
            op_tok = self._peek()
            if op_tok and op_tok.kind in _OPS:
                self._consume(op_tok.kind)
                rhs = self._value()
                return _Compare(path=path, op=_OPS[op_tok.kind], rhs=rhs)
            return _Truthy(path=path)
        raise ValueError(f"unexpected token {tok.kind} {tok.value!r}")

    def _path(self) -> tuple[str, ...]:
        parts = [self._consume("IDENT").value]
        while (tok := self._peek()) is not None and tok.kind == "DOT":
            self._consume("DOT")
            parts.append(self._consume("IDENT").value)
        return tuple(parts)

    def _value(self) -> Any:
        tok = self._peek()
        if tok is None:
            raise ValueError("expected value, found end of expression")
        if tok.kind == "STRING":
            self._consume("STRING")
            return tok.value
        if tok.kind == "NUMBER":
            self._consume("NUMBER")
            if "." in tok.value:
                return float(tok.value)
            return int(tok.value)
        if tok.kind == "IDENT":
            self._consume("IDENT")
            return tok.value
        raise ValueError(f"unexpected token in value: {tok.kind}")


_OPS = {
    "EQ": "==",
    "NEQ": "!=",
    "REQ": "=~",
    "NREQ": "!~",
    "GT": ">",
    "LT": "<",
    "GE": ">=",
    "LE": "<=",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClaimsExpr:
    """Compiled boolean expression evaluable against a claims dict."""

    _root: _Node

    def evaluate(self, ctx: Mapping[str, Any]) -> bool:
        return self._root.evaluate(ctx)


@lru_cache(maxsize=512)
def compile_expr(src: str) -> ClaimsExpr:
    """Compile a claims-expression source string into an evaluator.

    Repeated calls with the same source return the same compiled object
    (LRU cache, capacity 512).
    """
    if not src or not src.strip():
        raise ValueError("empty expression")
    tokens = _lex(src)
    if not tokens:
        raise ValueError("empty expression")
    return ClaimsExpr(_root=_Parser(tokens).parse())
