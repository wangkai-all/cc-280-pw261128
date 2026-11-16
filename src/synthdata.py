"""
synthdata.py —— 合成数据集生成器，带可断言的 ground truth。

覆盖的类型现象：
- 数值 / 数值字符串 / 普通字符串同族与跨族冲突
- bool 与 int 的经典混淆（必须区分）
- 字段缺失（missing）与显式 null
- 嵌套对象、对象数组、异构数组（标量与对象混排）
- 空对象 / 空数组边界
- 深层嵌套边界（深度 N）
- 「采样没看到、后面才出现」的延迟类型（late type）
- 坏行反例（非法 JSON、NaN 常量、重复键等由解析器分别处理）

ground truth 记录每个路径的期望标签集合与计数，供 tests 做精度断言。
"""

from __future__ import annotations

import json
import random
from typing import Any, Dict, List, Tuple

from . import typesys


def build_main_dataset(seed: int = 26) -> Tuple[List[dict], dict]:
    """返回 (records, ground_truth)。"""
    rng = random.Random(seed)
    records: List[dict] = []

    # 100 行主体：price 以数值为主，夹杂 NUMSTR；tier 是干净字符串；
    # active 是 bool；score 偶发缺失；meta 是嵌套对象；tags 是异构数组
    for i in range(100):
        rec = {
            "id": i,
            "name": f"item_{i:03d}",
            "price": round(rng.uniform(1, 1000), 2),
            "tier": rng.choice(["A", "B", "C"]),
            "active": rng.choice([True, False]),
            "tags": [
                rng.choice(["x", "y", "z"]),
                {"tag_id": rng.randint(1, 9), "weight": rng.random()},
            ],
            "meta": {
                "source": rng.choice(["api", "file", "manual"]),
                "ref": rng.randint(1000, 9999),
            },
        }
        # 约 8% 的 price 是数值字符串（NUMSTR）
        if i % 12 == 0:
            rec["price"] = f"{rec['price']:.2f}"
        # score：约 15% 缺失，5% 显式 null，其余 int
        if i % 7 != 0:
            rec["score"] = None if i % 19 == 0 else rng.randint(0, 100)
        # 空容器边界
        if i == 0:
            rec["empty_obj"] = {}
            rec["empty_arr"] = []
        records.append(rec)

    # 延迟类型：前 100 行 name 全是 STR，第 101 行突然变成对象 ——
    # 若只采样前几十行就锁类型，这里必须被识别为冲突而不是静默截断
    records.append({
        "id": 101,
        "name": {"raw": "late-object", "lang": "en"},
        "price": 50,
        "tier": "A",
        "active": True,
        "tags": ["z"],
        "meta": {"source": "api", "ref": 1},
        "score": 9,
    })
    # 延迟类型 2：active 从 BOOL 突变成 STR
    records.append({
        "id": 102, "name": "item_102", "price": 12, "tier": "B",
        "active": "yes", "tags": [], "meta": {"source": "file", "ref": 2},
    })
    # 深度嵌套边界（深度 8）
    deep: Any = "deep-leaf"
    for d in range(8, 0, -1):
        deep = {f"lvl{d}": deep}
    records.append({
        "id": 103, "name": "item_103", "price": 1.5, "tier": "C",
        "active": False, "tags": [1, "two", None, {"k": "v"}],
        "meta": {"source": "manual", "ref": 3}, "deep": deep,
    })
    # 标量数组与 null 元素
    records.append({
        "id": 104, "name": "item_104", "price": 2, "tier": "A",
        "active": True, "tags": [None, None], "meta": {"source": "api",
        "ref": 4}, "score": None,
    })

    ground_truth = _derive_ground_truth(records)
    return records, ground_truth


def _derive_ground_truth(records: List[dict]) -> dict:
    """用 typesys 自身统计，但断言时测试会独立核对关键路径的手工期望。"""
    acc = typesys.InferenceAccumulator()
    for r in records:
        acc.add_record(r)
    gt = {"total_rows": len(records), "paths": {}}
    for path, stats in acc.paths.items():
        gt["paths"][path] = {
            "type_counts": dict(stats["counts"]),
            "present": stats["present"],
            "missing": len(records) - stats["present"],
            "promised": list(typesys.promised_tags(stats)),
        }
    # 人工锚点：这些是测试必须独立命中的关键事实
    gt["manual_assertions"] = {
        "$.active": {"must_include": ["BOOL", "STR"],
                     "bool_int_separated": True},
        "$.id": {"promised": ["INT"]},
        "$.price": {"must_include": ["FLOAT", "NUMSTR"]},
        "$.name": {"must_include": ["STR", "OBJECT"]},
        "$.score": {"has_null": True, "has_missing": True},
        "$.tags": {"must_include": ["ARRAY"]},
        "$.tags[1]": {"must_include": ["OBJECT", "NULL", "STR"]},
    }
    return gt


def build_bad_lines() -> str:
    """反例文件：混合好行与各种坏行，解析器必须逐行报错且保住好行。"""
    lines = [
        json.dumps({"id": 1, "v": 1}),
        '{"id": 2, "v": 2,}',          # 尾逗号
        '{"id": 3, "v": NaN}',         # 非法常量
        'not-json-at-all',
        '{"id": 4, "v": 4',            # 未闭合
        '{"id": 5, "v": 5}',
        '{"id": 6, "v": 1e99999}',     # 溢出为 Infinity，拒绝
        '{"id": 7, "v": 7}',
    ]
    return "\n".join(lines)


def build_late_type_stream() -> str:
    """
    延迟类型流：前 30 行字段只有 INT，第 31 行出现 STR 和 OBJECT。
    用于验证「采样没看到的类型后面出现时不能静默截断」。
    """
    lines = [json.dumps({"user_id": i, "payload": i * 2})
             for i in range(30)]
    lines.append(json.dumps({"user_id": "A-31", "payload": {"k": "v"}}))
    lines.append(json.dumps({"user_id": 32, "payload": 32}))
    return "\n".join(lines)


def to_jsonl(records: List[dict]) -> str:
    return "\n".join(
        json.dumps(r, ensure_ascii=False, separators=(",", ":"))
        for r in records
    )
