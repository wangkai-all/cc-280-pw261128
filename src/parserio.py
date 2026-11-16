"""
parserio.py —— 原始 JSON 导入解析。

支持两种形态并可在同一份文本里自动识别：
1. 整个文件是一个 JSON 数组（[{"...": ...}, ...]）
2. JSONL：每行一个独立 JSON 值

反例文件里允许存在坏行：坏行不会炸掉整个导入，而是作为带行号的
解析错误返回，由工作台决定是否展示/丢弃；坏数据永远不会悄悄变成空值。

严格性：使用 parse_constant 拒绝 NaN/Infinity（它们不是合法 JSON 标量）。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, List, Optional

# 裸的非标准常量（Python json 默认会接受它们，必须显式拒绝）
_FORBIDDEN_TOKEN = re.compile(r"\b(?:NaN|Infinity|-Infinity)\b")
# 溢出量级的数字字面量：解析后会变成 Infinity，同样拒绝
_EXP_OVERFLOW = re.compile(r"[eE][+]?\d{4,}")


@dataclass
class ParseError:
    line: int                 # 1-based 行号
    snippet: str
    message: str


@dataclass
class ParseResult:
    records: List[Any] = field(default_factory=list)
    errors: List[ParseError] = field(default_factory=list)
    format_detected: str = ""          # "array" | "jsonl"
    raw_lines: List[str] = field(default_factory=list)  # 与 records 对齐的原文

    @property
    def ok(self) -> bool:
        return not self.errors


def _reject_constant(token: str):
    raise ValueError(f"非标准 JSON 常量被拒绝：{token}")


def _guard_nonstandard(text: str) -> None:
    """预扫描：拒绝 NaN/Infinity 裸常量与会溢出为 Infinity 的数字。"""
    if _FORBIDDEN_TOKEN.search(text):
        raise ValueError("文本包含非标准 JSON 常量（NaN/Infinity），已拒绝")
    for m in _EXP_OVERFLOW.finditer(text):
        if int(m.group(0)[1:].lstrip("+")) > 308:
            raise ValueError(
                f"数字指数量级 {m.group(0)} 会溢出为 Infinity，已拒绝"
            )


def _strict_loads(text: str) -> Any:
    _guard_nonstandard(text)
    return json.loads(text, parse_constant=_reject_constant,
                      parse_int=int, parse_float=float)


def parse_input(text: str) -> ParseResult:
    """解析用户粘贴/上传的原始文本，绝不因单条坏数据抛出整体异常。"""
    stripped = text.strip()
    if not stripped:
        return ParseResult()

    # 形态 1：整体 JSON 数组
    if stripped[0] == "[":
        result = ParseResult(format_detected="array")
        try:
            payload = _strict_loads(stripped)
        except ValueError as exc:
            # 数组整体坏掉：定位失败，整份作为一个错误返回，不伪造记录
            result.errors.append(
                ParseError(
                    line=_guess_error_line(str(exc)),
                    snippet=stripped[:200],
                    message=f"JSON 解析失败：{exc}",
                )
            )
            return result
        if isinstance(payload, list):
            for idx, item in enumerate(payload):
                result.records.append(item)
                result.raw_lines.append(
                    json.dumps(item, ensure_ascii=False,
                               separators=(",", ":"))
                )
        else:
            # 顶层是单个非数组值：作为一条记录保留
            result.records.append(payload)
            result.raw_lines.append(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            )
        return result

    # 单行且以 { 开头：整体单个 JSON 对象
    if stripped[0] == "{" and "\n" not in stripped:
        result = ParseResult(format_detected="json")
        try:
            payload = _strict_loads(stripped)
        except ValueError as exc:
            result.errors.append(
                ParseError(line=_guess_error_line(str(exc)),
                           snippet=stripped[:200],
                           message=f"JSON 解析失败：{exc}"))
            return result
        result.records.append(payload)
        result.raw_lines.append(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return result

    # 形态 2：JSONL（多行逐行 JSON，允许坏行逐行报错）
    result = ParseResult(format_detected="jsonl")
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            value = _strict_loads(line)
        except ValueError as exc:
            result.errors.append(
                ParseError(
                    line=lineno,
                    snippet=line[:200],
                    message=str(exc),
                )
            )
            continue
        result.records.append(value)
        result.raw_lines.append(line)
    return result


def _guess_error_line(message: str) -> int:
    # Python 的 json 错误信息通常带 "char X"；无法定位时返回 1
    import re

    m = re.search(r"char (\d+)", message)
    if not m:
        return 1
    return message.count("\n", 0, int(m.group(1))) + 1


def records_to_jsonl(records: List[Any]) -> str:
    return "\n".join(
        json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records
    )
