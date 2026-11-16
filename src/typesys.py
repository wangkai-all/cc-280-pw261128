"""
typesys.py —— 原始 JSON 的类型分类、推断累加、联合结构与处置策略。

设计原则：
1. 类型承诺必须可解释：每个字段路径得到一个显式类型集合（联合），而不是
   悄悄把所有东西转成文本。
2. 不静默截断：推断阶段看到的每个类型都被计数；入仓阶段遇到新类型必须
   触发隔离/影子处置，不允许丢精度后假装兼容。
3. bool 不并入 int（Python 的历史陷阱在这里显式拒绝）。
4. "123" 这类数值字符串单独标记为 NUMSTR，与普通字符串 STR 区分，
   使「可确定性重解析」成为一等信息。

类型标签：
  NULL / BOOL / INT / FLOAT / NUMSTR / STR / ARRAY / OBJECT
联合类型用排序后的元组表示，如 ("FLOAT", "INT")。
"""

from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterator, List, Optional, Tuple

# ---- 类型标签 ---------------------------------------------------------------

NULL = "NULL"
BOOL = "BOOL"
INT = "INT"
FLOAT = "FLOAT"
NUMSTR = "NUMSTR"   # 内容可被确定性重解析为数字的字符串
STR = "STR"
ARRAY = "ARRAY"
OBJECT = "OBJECT"

SCALAR_TAGS = (NULL, BOOL, INT, FLOAT, NUMSTR, STR)
ALL_TAGS = (NULL, BOOL, INT, FLOAT, NUMSTR, STR, ARRAY, OBJECT)

# 处置策略
REJECT = "REJECT"      # 严格拒收：不满足承诺的行进隔离区
UNION = "UNION"        # 保留联合：变体值原样入仓并带类型判别
SHADOW = "SHADOW"      # 影子新列：主列保持承诺类型，异类值进影子列

DISPOSITIONS = (REJECT, UNION, SHADOW)

# int -> float 的拓宽规则（数值家族内部的安全拓宽，不算冲突）
WIDEN = {
    (INT, FLOAT): FLOAT,
    (FLOAT, INT): FLOAT,
}

_INTLIKE = re.compile(r"^[+-]?\d+$")
_FLOATLIKE = re.compile(
    r"^[+-]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?$"
)


class TypeSystemError(Exception):
    """类型系统基础异常。"""


class SchemaViolation(TypeSystemError):
    """值违反已确认的类型承诺。携带路径、实际标签、承诺集合。"""

    def __init__(self, path: str, actual: str, promised: Tuple[str, ...]):
        self.path = path
        self.actual = actual
        self.promised = promised
        super().__init__(
            f"路径 {path!r} 的实际类型 {actual} 不在承诺集合 {list(promised)} 中"
        )


class UnknownType(TypeSystemError):
    """入仓时遇到推断阶段从未出现过的类型标签。"""

    def __init__(self, path: str, actual: str, known: Tuple[str, ...]):
        self.path = path
        self.actual = actual
        self.known = known
        super().__init__(
            f"路径 {path!r} 出现采样未见过的类型 {actual}（已知：{list(known)}）"
        )


# ---- 单值分类 ---------------------------------------------------------------

def classify(value: Any) -> str:
    """把一个 Python 值映射到 8 个类型标签之一。

    注意：bool 必须在 int 之前判断（True/False 是 int 的子类）。
    """
    if value is None:
        return NULL
    if isinstance(value, bool):
        return BOOL
    if isinstance(value, int):
        return INT
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            # NaN/Infinity 不是合法 JSON 标量，拒绝静默吞掉
            raise TypeSystemError("NaN/Infinity 不是合法 JSON 标量值")
        return FLOAT
    if isinstance(value, str):
        return NUMSTR if is_numeric_string(value) else STR
    if isinstance(value, list):
        return ARRAY
    if isinstance(value, dict):
        return OBJECT
    raise TypeSystemError(f"无法分类的值类型：{type(value).__name__}")


def is_numeric_string(s: str) -> bool:
    """字符串是否可被确定性重解析为数字（int 或 float 语法）。"""
    if not s or len(s) > 64:
        return False
    if _INTLIKE.match(s):
        return True
    return bool(_FLOATLIKE.match(s)) and _looks_like_number(s)


def _looks_like_number(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def numeric_string_kind(s: str) -> str:
    """NUMSTR 的子形态：intlike / floatlike。"""
    if _INTLIKE.match(s):
        return "intlike"
    return "floatlike"


# ---- 路径展开（嵌套结构保真）------------------------------------------------

PATH_ROOT = "$"


def join_path(prefix: str, key: str) -> str:
    if prefix == PATH_ROOT:
        return f"$.{key}"
    return f"{prefix}.{key}"


def flatten(record: Any, prefix: str = PATH_ROOT) -> Iterator[Tuple[str, Any]]:
    """
    深度优先展开一条记录，产出 (path, value)。

    - dict 的每个键展开为子路径；空 dict 保留 OBJECT 叶子。
    - list 展开为 [i] 子路径；空 list 保留 ARRAY 叶子。
    - 标量即叶子。
    路径集合本身就是嵌套结构的完整描述，下游可据此重建结构。
    """
    yield prefix, record
    if isinstance(record, dict):
        for k in sorted(record.keys()):
            yield from flatten(record[k], join_path(prefix, k))
    elif isinstance(record, list):
        for i, item in enumerate(record):
            yield from flatten(item, f"{prefix}[{i}]")


# ---- 推断累加器 -------------------------------------------------------------

def empty_stats() -> dict:
    return {
        "counts": {},                       # tag -> 出现次数（含 NULL）
        "numstr_kinds": {"intlike": 0, "floatlike": 0},
        "samples": {},                      # tag -> 一个样本值（可解释性）
        "present": 0,                       # 该路径出现的行数
    }


class InferenceAccumulator:
    """
    流式推断器：逐行累加，永不丢类型。

    可 merge 另一个累加器（持续接入/分片推断），可序列化到 JSON 存储，
    旧任务结果与新任务结果通过 merge 汇合而不是互相覆盖。
    """

    def __init__(self) -> None:
        self.total_rows = 0
        self.paths: Dict[str, dict] = {}

    def add_record(self, record: Any) -> None:
        self.total_rows += 1
        seen = set()
        for path, value in flatten(record):
            if path not in seen:
                seen.add(path)
                stats = self.paths.setdefault(path, empty_stats())
                stats["present"] += 1
            else:
                stats = self.paths.setdefault(path, empty_stats())
            tag = classify(value)
            stats["counts"][tag] = stats["counts"].get(tag, 0) + 1
            if tag == NUMSTR:
                stats["numstr_kinds"][numeric_string_kind(value)] += 1
            if tag not in stats["samples"]:
                stats["samples"][tag] = _sample_repr(value)

    def merge(self, other: "InferenceAccumulator") -> None:
        """合并另一个累加器（分片/并发推断汇合），只做加法，不覆盖。"""
        self.total_rows += other.total_rows
        for path, ostats in other.paths.items():
            stats = self.paths.setdefault(path, empty_stats())
            stats["present"] += ostats["present"]
            for tag, n in ostats["counts"].items():
                stats["counts"][tag] = stats["counts"].get(tag, 0) + n
            for kind, n in ostats["numstr_kinds"].items():
                stats["numstr_kinds"][kind] = (
                    stats["numstr_kinds"].get(kind, 0) + n
                )
            for tag, sample in ostats["samples"].items():
                stats["samples"].setdefault(tag, sample)

    def to_dict(self) -> dict:
        return {
            "total_rows": self.total_rows,
            "paths": {
                path: {
                    "counts": dict(stats["counts"]),
                    "numstr_kinds": dict(stats["numstr_kinds"]),
                    "samples": dict(stats["samples"]),
                    "present": stats["present"],
                }
                for path, stats in sorted(self.paths.items())
            },
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "InferenceAccumulator":
        acc = cls()
        acc.total_rows = payload.get("total_rows", 0)
        for path, stats in payload.get("paths", {}).items():
            acc.paths[path] = {
                "counts": dict(stats.get("counts", {})),
                "numstr_kinds": dict(
                    stats.get("numstr_kinds", {"intlike": 0, "floatlike": 0})
                ),
                "samples": dict(stats.get("samples", {})),
                "present": stats.get("present", 0),
            }
        return acc


def _sample_repr(value: Any, limit: int = 80) -> Any:
    """样本值用于页面展示；超长字符串截断并标注（仅展示层截断）。"""
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "…"
    if isinstance(value, (list, dict)):
        text = _json_compact(value)
        if len(text) > limit:
            return text[:limit] + "…"
        return value
    return value


def _json_compact(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


# ---- 推断报告：联合类型、缺失率、处置建议 ------------------------------------

def non_null_tags(stats: dict) -> Tuple[str, ...]:
    return tuple(
        sorted(t for t in stats["counts"].keys() if t != NULL)
    )


def promised_tags(stats: dict) -> Tuple[str, ...]:
    """推断出的承诺集合：非空标签经过数值拓宽后的最小集合，NULL 始终允许。"""
    tags = set(non_null_tags(stats))
    if INT in tags and FLOAT in tags:
        tags.discard(INT)  # int/float 共存 -> 承诺 FLOAT（安全拓宽）
    return tuple(sorted(tags))


def is_single_family(tags: Tuple[str, ...]) -> bool:
    """非空类型是否属于同一个可安全合并的家族。"""
    if len(tags) <= 1:
        return True
    return tuple(sorted(tags)) in (("FLOAT", "INT"),)


def recommend_disposition(stats: dict, total_rows: int) -> dict:
    """
    给出处置建议与理由（建议可被人工在页面上覆盖）。

    适用条件（工作台把取舍显式化）：
    - REJECT  单一类型家族，或冲突占比极低且异类明显是脏数据；
              下游要求强类型、查询可优化，拒收行全部可解释地进隔离区。
    - UNION   冲突是真实业务语义（同一字段合法地承载多种类型），
              下游能处理变体；信息零损失，但聚合查询需要按判别子分流。
    - SHADOW  一个主导类型 + 少量异类：主列维持强类型与查询能力，
              异类值零丢失地进影子列，主分析不会被静默污染也不会被截断。
    """
    tags = non_null_tags(stats)
    present = stats["present"]
    counts = stats["counts"]
    non_null_total = sum(n for t, n in counts.items() if t != NULL)

    # 纯空/全 NULL：承诺 NULL，默认 UNION 最保守
    if not tags:
        return {
            "disposition": UNION,
            "promised": (),
            "reason": "该路径只有 NULL/缺失，暂不承诺非空类型，保守保留联合结构",
            "conflict_ratio": 0.0,
        }

    promised = promised_tags(stats)
    conflict_ratio = 0.0
    if non_null_total:
        # 拓宽后，落在承诺集合外的非空值占比
        outside = sum(
            n
            for t, n in counts.items()
            if t != NULL and t not in promised
        )
        conflict_ratio = outside / non_null_total

    if is_single_family(tags):
        return {
            "disposition": REJECT,
            "promised": promised,
            "reason": "单一类型家族（INT/FLOAT 内部拓宽不算冲突），强类型承诺零信息损失",
            "conflict_ratio": 0.0,
        }

    # 多家族冲突
    dominant = max(
        ((t, counts.get(t, 0)) for t in tags), key=lambda kv: kv[1]
    )
    dominant_tag, dominant_n = dominant
    dominant_ratio = dominant_n / non_null_total if non_null_total else 0.0

    # 数值 vs 数值字符串：可确定性重解析，冲突温和
    numeric_family = {INT, FLOAT, NUMSTR}
    if set(tags) <= numeric_family:
        if dominant_ratio >= 0.9:
            return {
                "disposition": SHADOW,
                "promised": promised,
                "reason": (
                    f"{dominant_tag} 占主导（{dominant_ratio:.1%}），数值字符串可确定性重解析；"
                    "主列保留数值查询能力，异类进影子列可回溯"
                ),
                "conflict_ratio": conflict_ratio,
            }
        return {
            "disposition": UNION,
            "promised": promised,
            "reason": "数值与数值字符串均为合法语义且占比接近，保留联合 + 判别子，零损失",
            "conflict_ratio": conflict_ratio,
        }

    # 标量与容器（ARRAY/OBJECT）互现：无法安全合并
    if ARRAY in tags or OBJECT in tags:
        return {
            "disposition": UNION,
            "promised": promised,
            "reason": "标量与容器类型互现，结构上不可互相拓宽，只能保留联合或在源头拆分字段",
            "conflict_ratio": conflict_ratio,
        }

    # 一般标量冲突（如 STR 与 BOOL/NUMSTR）
    if dominant_ratio >= 0.95:
        return {
            "disposition": REJECT,
            "promised": promised,
            "reason": (
                f"主导类型 {dominant_tag} 占 {dominant_ratio:.1%}，异类极稀有且语义可疑；"
                "严格拒收并在隔离区保留原值，适合强类型下游"
            ),
            "conflict_ratio": conflict_ratio,
        }
    if dominant_ratio >= 0.8:
        return {
            "disposition": SHADOW,
            "promised": promised,
            "reason": (
                f"主导类型 {dominant_tag} 占 {dominant_ratio:.1%}，影子列兼顾主分析查询能力与异类保真"
            ),
            "conflict_ratio": conflict_ratio,
        }
    return {
        "disposition": UNION,
        "promised": promised,
        "reason": "多类型势均力敌，没有安全的单一承诺，联合结构是唯一零损失选择",
        "conflict_ratio": conflict_ratio,
    }


def build_report(acc: InferenceAccumulator) -> dict:
    """把累加器转成可展示、可存储、可确认的推断报告。"""
    report_paths = {}
    for path, stats in acc.paths.items():
        tags = non_null_tags(stats)
        promised = promised_tags(stats)
        advice = recommend_disposition(stats, acc.total_rows)
        present = stats["present"]
        report_paths[path] = {
            "path": path,
            "present_rows": present,
            "missing_rows": acc.total_rows - present,
            "missing_ratio": (acc.total_rows - present) / acc.total_rows
            if acc.total_rows
            else 0.0,
            "type_counts": dict(sorted(stats["counts"].items())),
            "numstr_kinds": dict(stats["numstr_kinds"]),
            "non_null_tags": list(tags),
            "promised_tags": list(promised),
            "single_family": is_single_family(tags),
            "samples": stats["samples"],
            "advice": advice,
        }
    return {
        "total_rows": acc.total_rows,
        "path_count": len(report_paths),
        "paths": report_paths,
    }
