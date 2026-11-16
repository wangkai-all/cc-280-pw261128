"""
engine.py —— 推断执行、持续接入、结构确认并发控制、入仓转换与查询。

并发与恢复的关键不变量：
1. 已确认结构 append-only，任何任务都没有修改已确认版本的代码路径。
2. 草稿用 guard（CAS 令牌）保护：迟到的旧推断任务拿过期 guard 写草稿
   会抛 GuardConflict；它的推断事实仍只追加进 inference_runs，
   并通过 InferenceAccumulator.merge 汇入最新累加器，而不是回退结构。
3. 任务按 row_index 检查点推进；cell 用 UPSERT、quarantine 按
   (version,row) 去重，恢复重放天然幂等。
4. 只有 status=committed 的任务产出被视为已发布；quarantine.published
   恒为 0，失败恢复绝不会把隔离行当已发布数据。
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from . import typesys
from .storage import GuardConflict, Storage, compute_guard, new_id

# 转换结果的处置码
CODE_OK = "ok"                  # 符合承诺，正常入主列
CODE_COERCED = "coerced"        # NUMSTR 等确定性重解析后入主列
CODE_UNION = "union"            # 联合：异类带判别入主列
CODE_SHADOW = "shadow"          # 影子：异类原值进影子列
CODE_REJECT = "reject"          # 拒收：行进隔离区
CODE_NULL = "null"              # NULL/缺失：允许，写空值


# ============================================================================
# 单值转换：工作台的核心可解释单元
# ============================================================================

def coerce_value(value: Any, actual: str, promised: Tuple[str, ...]
                 ) -> Tuple[Any, str]:
    """
    按承诺集合做确定性转换。只允许无损或可解释的转换：
    - INT -> FLOAT 是数值拓宽；
    - NUMSTR -> INT/FLOAT 是确定性重解析（字符串与解析值都保留）。
    其他任何跨家族转换（比如 STR -> INT、BOOL -> STR）一律拒绝，
    不做「全转文本」式假兼容。
    """
    if actual in promised:
        return value, CODE_OK
    # INT/FLOAT 互相拓宽
    if actual == typesys.INT and typesys.FLOAT in promised:
        return float(value), CODE_COERCED
    if actual == typesys.FLOAT and typesys.INT in promised:
        # float -> int 可能丢精度，拒绝
        raise typesys.SchemaViolation("", actual, promised)
    # NUMSTR 的确定性重解析
    if actual == typesys.NUMSTR:
        if typesys.INT in promised and typesys.numeric_string_kind(value) == "intlike":
            return int(value), CODE_COERCED
        if typesys.FLOAT in promised:
            return float(value), CODE_COERCED
    raise typesys.SchemaViolation("", actual, promised)


def convert_path(value: Any, path_spec: dict) -> dict:
    """
    对一个 (path, value) 应用该路径的处置策略，返回结构化转换结果。

    返回字段：code / actual / promised / disposition / main / shadow /
             coerced / reason
    这是页面「逐行转换」视图与实际入仓共享的同一套逻辑，预览即所得。
    """
    disposition = path_spec["disposition"]
    promised = tuple(path_spec["promised_tags"])
    actual = typesys.classify(value)
    result = {
        "path": path_spec["path"],
        "actual": actual,
        "promised": list(promised),
        "disposition": disposition,
        "main": None,
        "shadow": None,
        "coerced": None,
        "code": None,
        "reason": "",
        "raw": value,
    }

    if actual == typesys.NULL:
        result["code"] = CODE_NULL
        result["main"] = None
        result["reason"] = "NULL 对任何承诺都合法"
        return result

    # 采样未见过的类型：known 集合里没有 actual -> 绝不静默截断。
    # 即使是 UNION，也不能凭空接纳推断阶段从未出现的类型，
    # 必须进隔离区并要求新版本承接。
    known = set(path_spec.get("known_tags", promised))
    if actual not in known and actual not in promised:
        raise typesys.UnknownType(path_spec["path"], actual,
                                  tuple(sorted(known)))

    if disposition == typesys.REJECT:
        try:
            coerced, _ = coerce_value(value, actual, promised)
        except typesys.SchemaViolation:
            result["code"] = CODE_REJECT
            result["reason"] = (
                f"严格拒收：已观察的 {actual} 不在承诺 {list(promised)} 中，"
                "原值进隔离区可回溯"
            )
            return result
        result["code"] = CODE_COERCED if actual not in promised else CODE_OK
        result["main"] = coerced
        result["coerced"] = coerced
        return result

    if disposition == typesys.UNION:
        if actual in promised:
            result["code"] = CODE_OK
            result["main"] = value
        else:
            # 联合承诺：推断阶段已观察到的异类，原值 + 判别子保留，零损失
            result["code"] = CODE_UNION
            result["main"] = {"__kind__": actual, "value": value}
            result["reason"] = (
                f"联合保留：已观察的 {actual} 作为变体带判别子入仓，不丢原值"
            )
        return result

    if disposition == typesys.SHADOW:
        try:
            coerced, _ = coerce_value(value, actual, promised)
            result["code"] = CODE_COERCED if actual not in promised else CODE_OK
            result["main"] = coerced
            result["coerced"] = coerced
        except typesys.SchemaViolation:
            result["code"] = CODE_SHADOW
            result["shadow"] = {"__kind__": actual, "value": value}
            result["reason"] = (
                f"影子列：主列维持 {list(promised)}，{actual} 原值零损失进影子列"
            )
        return result

    raise ValueError(f"未知处置策略：{disposition}")


def preview_row(record: Any, spec: dict) -> List[dict]:
    """页面试用：对一条原始记录跑完整逐路径转换，不落库。

    未知类型不抛异常，而是产出 code=reject 的结构化结果（页面可渲染、
    入仓时同一判断会把行进隔离区），保证「不静默截断」对下游可见。
    """
    out = []
    for path, value in typesys.flatten(record):
        path_spec = spec["paths"].get(path)
        if path_spec is None:
            # 结构确认后新冒出的路径：显式标注，不假装不存在
            out.append({
                "path": path, "actual": typesys.classify(value),
                "promised": [], "disposition": None,
                "main": None, "shadow": None, "coerced": None,
                "code": CODE_REJECT,
                "reason": "结构确认后出现的未知路径，需新版本承接",
                "raw": value,
            })
            continue
        try:
            out.append(convert_path(value, path_spec))
        except typesys.UnknownType as exc:
            out.append({
                "path": path, "actual": exc.actual,
                "promised": list(exc.known),
                "disposition": path_spec["disposition"],
                "main": None, "shadow": None, "coerced": None,
                "code": CODE_REJECT,
                "reason": (
                    f"采样未见类型 {exc.actual}，拒绝静默截断/转文本，"
                    "入仓时该行进隔离区"
                ),
                "raw": value,
            })
    return out


# ============================================================================
# 规格构建：把推断报告变成可确认的结构承诺
# ============================================================================

def dominant_tag(type_counts: dict) -> Optional[str]:
    """出现次数最多的非空标签（计数相同按标签名确定性决胜）。"""
    non_null = {t: n for t, n in type_counts.items() if t != typesys.NULL}
    if not non_null:
        return None
    return max(sorted(non_null), key=lambda t: non_null[t])


def effective_promise(type_counts: dict, disposition: str) -> Tuple[str, ...]:
    """
    给定观察计数与处置策略，推导有效承诺集合。

    - UNION：承诺全部观察到的非空类型（INT/FLOAT 内部仍拓宽）。
    - REJECT/SHADOW：承诺收窄到主导类型所在家族：
      * 数值家族（INT/FLOAT/NUMSTR）整体保留——NUMSTR 可确定性重解析，
        与数值同属一个可无损互转的家族，不算跨家族冲突；
      * 其他家族只承诺主导标签，异类被拒收或进影子列。
    这让「严格拒收」产生真正的强类型主列，而不是名义 REJECT 实则全放行。
    """
    numeric = {typesys.INT, typesys.FLOAT, typesys.NUMSTR}
    tags = tuple(sorted(t for t in type_counts if t != typesys.NULL))
    full = set(tags)
    if typesys.INT in full and typesys.FLOAT in full:
        full.discard(typesys.INT)
    if disposition == typesys.UNION or not tags:
        return tuple(sorted(full))

    dom = dominant_tag(type_counts)
    if dom in numeric and (set(tags) & numeric):
        # 数值家族主导：只把实际观察到的数值类型纳入承诺
        promise = set()
        if typesys.FLOAT in tags:
            promise.add(typesys.FLOAT)       # FLOAT 存在则 INT 无损拓宽进来
        elif typesys.INT in tags:
            promise.add(typesys.INT)         # 只有 INT 时承诺保持 INT
        if typesys.NUMSTR in tags:
            promise.add(typesys.NUMSTR)      # NUMSTR 确定性重解析，同家族保留
        return tuple(sorted(promise))
    return (dom,)


def build_spec_from_report(report: dict,
                           overrides: Optional[Dict[str, str]] = None) -> dict:
    """
    每个路径一条承诺：promised_tags + disposition + 已知标签全集。
    overrides 允许人工在页面上把建议改成 REJECT/UNION/SHADOW；
    选择 REJECT/SHADOW 时承诺自动收窄到主导类型。
    """
    overrides = overrides or {}
    paths = {}
    for path, item in report["paths"].items():
        disposition = overrides.get(path, item["advice"]["disposition"])
        if disposition not in typesys.DISPOSITIONS:
            raise ValueError(f"非法处置策略：{disposition}")
        paths[path] = {
            "path": path,
            "promised_tags": list(
                effective_promise(item["type_counts"], disposition)),
            "known_tags": list(item["non_null_tags"]),
            "disposition": disposition,
            "type_counts": item["type_counts"],
            "missing_ratio": item["missing_ratio"],
            "advice": item["advice"]["disposition"],
            "reason": item["advice"]["reason"],
        }
    return {
        "paths": paths,
        "source_total_rows": report["total_rows"],
    }


# ============================================================================
# 任务编排
# ============================================================================

class Engine:
    def __init__(self, storage: Storage, max_workers: int = 4):
        self.storage = storage
        self._pool = ThreadPoolExecutor(max_workers=max_workers,
                                        thread_name_prefix="job")
        self._run_lock = threading.RLock()

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)

    # ---- 推断 -------------------------------------------------------------

    def start_inference(self, dataset_id: str,
                        base_run_id: Optional[str] = None,
                        fail_after: Optional[int] = None) -> str:
        """提交异步推断任务。fail_after 仅供故障注入测试。"""
        job_id = self.storage.create_job(
            dataset_id, "infer",
            {"base_run_id": base_run_id, "fail_after": fail_after},
        )
        self._pool.submit(self._run_inference, job_id, dataset_id,
                          base_run_id, fail_after)
        return job_id

    def run_inference_sync(self, dataset_id: str,
                           base_run_id: Optional[str] = None,
                           fail_after: Optional[int] = None) -> dict:
        """同步执行推断（测试/脚本用），返回任务结果摘要。"""
        job_id = self.storage.create_job(
            dataset_id, "infer",
            {"base_run_id": base_run_id, "fail_after": fail_after},
        )
        return self._run_inference(job_id, dataset_id, base_run_id, fail_after)

    def _run_inference(self, job_id: str, dataset_id: str,
                       base_run_id: Optional[str],
                       fail_after: Optional[int]) -> dict:
        st = self.storage
        st.update_job(job_id, status="running")
        try:
            # 持续接入：从父快照恢复累加器，而不是从零开始
            if base_run_id:
                parent = self._load_run_acc(base_run_id)
                acc = typesys.InferenceAccumulator.from_dict(parent)
            else:
                acc = typesys.InferenceAccumulator()

            checkpoint = int(st.get_job(job_id)["checkpoint"] or 0)
            rows = st.iter_raw_records(dataset_id, after_index=checkpoint)
            processed = 0
            for row in rows:
                acc.add_record(row["record"])
                processed += 1
                st.update_job(job_id, checkpoint=row["row_index"] + 1)
                if fail_after is not None and processed >= fail_after:
                    raise RuntimeError(
                        f"故障注入：处理 {processed} 行后中断（检查点已持久化）"
                    )

            report = typesys.build_report(acc)
            run_id = st.save_inference_run(
                dataset_id, job_id, base_run_id, acc.total_rows,
                acc.to_dict(), report,
            )
            st.update_job(job_id, status="committed")
            st.audit(dataset_id, "infer.committed",
                     {"job_id": job_id, "run_id": run_id,
                      "rows": acc.total_rows})
            return {"job_id": job_id, "run_id": run_id,
                    "total_rows": acc.total_rows, "report": report}
        except Exception as exc:  #  noqa: BLE001 - 任务级失败必须落库
            st.update_job(job_id, status="failed", error=str(exc))
            st.audit(dataset_id, "infer.failed",
                     {"job_id": job_id, "error": str(exc)})
            return {"job_id": job_id, "error": str(exc)}

    def _load_run_acc(self, run_id: str) -> dict:
        with self.storage.conn:
            row = self.storage.conn.execute(
                "SELECT accumulator FROM inference_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if not row:
            raise ValueError(f"推断快照不存在：{run_id}")
        return json.loads(row["accumulator"])

    # ---- 草稿与确认（守卫 CAS）--------------------------------------------

    def draft_from_latest(self, dataset_id: str,
                          overrides: Optional[Dict[str, str]] = None) -> dict:
        run = self.storage.latest_inference_run(dataset_id)
        if not run:
            raise ValueError("还没有推断结果，无法生成草稿")
        spec = build_spec_from_report(run["report"], overrides)
        return self.storage.create_draft(dataset_id, run["run_id"], spec)

    def revise_draft(self, version_id: str, guard: str,
                     overrides: Dict[str, str]) -> dict:
        """人工修改处置策略。带守卫，防止覆盖并发修改。"""
        version = self.storage.get_version(version_id)
        if not version or version["status"] != "draft":
            raise ValueError("只能修改草稿版本")
        spec = version["spec"]
        for path, disposition in overrides.items():
            if path not in spec["paths"]:
                raise ValueError(f"规格中不存在路径：{path}")
            if disposition not in typesys.DISPOSITIONS:
                raise ValueError(f"非法处置策略：{disposition}")
            p = spec["paths"][path]
            p["disposition"] = disposition
            # 处置变了，有效承诺必须随之重算（REJECT/SHADOW 收窄到主导类型）
            p["promised_tags"] = list(
                effective_promise(p["type_counts"], disposition))
        return self.storage.update_draft(version_id, spec, guard)

    def confirm(self, version_id: str, guard: str) -> dict:
        return self.storage.confirm_version(version_id, guard)

    # ---- 入仓 --------------------------------------------------------------

    def start_ingest(self, dataset_id: str, version_id: str,
                     fail_after: Optional[int] = None) -> str:
        job_id = self.storage.create_job(
            dataset_id, "ingest",
            {"version_id": version_id, "fail_after": fail_after},
        )
        self._pool.submit(self._run_ingest, job_id, dataset_id,
                          version_id, fail_after)
        return job_id

    def run_ingest_sync(self, dataset_id: str, version_id: str,
                        fail_after: Optional[int] = None) -> dict:
        job_id = self.storage.create_job(
            dataset_id, "ingest",
            {"version_id": version_id, "fail_after": fail_after},
        )
        return self._run_ingest(job_id, dataset_id, version_id, fail_after)

    def _run_ingest(self, job_id: str, dataset_id: str, version_id: str,
                    fail_after: Optional[int]) -> dict:
        st = self.storage
        version = st.get_version(version_id)
        if not version:
            raise ValueError(f"版本不存在：{version_id}")
        if version["status"] != "confirmed":
            st.update_job(job_id, status="failed",
                          error="只能对已确认版本入仓")
            return {"job_id": job_id, "error": "版本未确认"}

        spec = version["spec"]
        st.update_job(job_id, status="running")
        already_quarantined = st.quarantine_row_ids(version_id)
        summary = {"processed": 0, "published_rows": 0, "quarantined_rows": 0,
                   "shadow_cells": 0, "union_cells": 0, "coerced_cells": 0,
                   "violations": []}
        try:
            checkpoint = int(st.get_job(job_id)["checkpoint"] or 0)
            rows = st.iter_raw_records(dataset_id, after_index=checkpoint)
            processed = 0
            for row in rows:
                row_id = row["row_id"]
                conv = preview_row(row["record"], spec)
                violations = [c for c in conv if c["code"] == CODE_REJECT]
                if violations:
                    # REJECT：整行进隔离区，主表不写任何 cell（严格分账）
                    summary["quarantined_rows"] += 1
                    if row_id not in already_quarantined:
                        for v in violations:
                            st.add_quarantine(
                                dataset_id, version_id, job_id, row_id,
                                v["path"], v["actual"], v["promised"],
                                v["reason"], json.dumps(v["raw"],
                                                         ensure_ascii=False),
                            )
                            summary["violations"].append({
                                "row_id": row_id, "path": v["path"],
                                "actual": v["actual"],
                            })
                        already_quarantined.add(row_id)
                else:
                    summary["published_rows"] += 1
                    for c in conv:
                        self._write_cell(dataset_id, version_id, row_id, c,
                                         summary)
                processed += 1
                st.update_job(job_id, checkpoint=row["row_index"] + 1)
                if fail_after is not None and processed >= fail_after:
                    raise RuntimeError(
                        f"故障注入：入仓 {processed} 行后中断（可从检查点恢复）"
                    )

            st.update_job(job_id, status="committed")
            st.audit(dataset_id, "ingest.committed",
                     {"job_id": job_id, "version_id": version_id, **summary})
            summary["job_id"] = job_id
            return summary
        except Exception as exc:  # noqa: BLE001
            st.update_job(job_id, status="failed", error=str(exc))
            st.audit(dataset_id, "ingest.failed",
                     {"job_id": job_id, "error": str(exc),
                      "checkpoint": st.get_job(job_id)["checkpoint"]})
            summary["job_id"] = job_id
            summary["error"] = str(exc)
            return summary

    def _write_cell(self, dataset_id: str, version_id: str, row_id: str,
                    conv: dict, summary: dict) -> None:
        path = conv["path"]
        st = self.storage
        if conv["code"] == CODE_NULL:
            st.upsert_cell(dataset_id, version_id, row_id, path, False,
                           typesys.NULL, "null", None)
            return
        if conv["code"] == CODE_SHADOW:
            # 影子：主列写 NULL 占位（承诺保持），异类原值进影子列
            st.upsert_cell(dataset_id, version_id, row_id, path, False,
                           typesys.NULL, "null", None)
            st.upsert_cell(
                dataset_id, version_id, row_id, path, True,
                conv["actual"],
                json.dumps(conv["shadow"], ensure_ascii=False), None,
            )
            summary["shadow_cells"] += 1
            return
        # OK / COERCED / UNION 都进主列
        st.upsert_cell(
            dataset_id, version_id, row_id, path, False, conv["actual"],
            json.dumps(conv["main"], ensure_ascii=False),
            json.dumps(conv["coerced"], ensure_ascii=False)
            if conv["coerced"] is not None else None,
        )
        if conv["code"] == CODE_UNION:
            summary["union_cells"] += 1
        elif conv["code"] == CODE_COERCED:
            summary["coerced_cells"] += 1

    # ---- 查询 --------------------------------------------------------------

    def query(self, dataset_id: str, version_id: str, path: str,
              op: str = "scan", value: Any = None,
              include_shadow: bool = True, limit: int = 200) -> dict:
        """
        在已发布单元格上查询。
        op: scan / eq / gt / lt / avg / count / min / max
        影子单元格默认单独成组返回，绝不和主列混算（避免静默污染聚合）。
        """
        st = self.storage
        with st._lock:
            rows = st.conn.execute(
                "SELECT row_id, is_shadow, value_type, value_json, coerced_json "
                "FROM cells WHERE dataset_id=? AND version_id=? AND path=? "
                "ORDER BY row_id",
                (dataset_id, version_id, path),
            ).fetchall()

        main, shadow = [], []
        for r in rows:
            item = {
                "row_id": r["row_id"],
                "type": r["value_type"],
                "value": _safe_json(r["value_json"]),
                "coerced": _safe_json(r["coerced_json"]),
            }
            (shadow if r["is_shadow"] else main).append(item)

        result = {"path": path, "op": op, "main_scanned": len(main),
                  "shadow_scanned": len(shadow), "rows": [],
                  "aggregate": None, "shadow_rows": []}

        if op in ("avg", "count", "min", "max"):
            result["aggregate"] = _aggregate(main, op)
        else:
            picked = _filter(main, op, value)
            result["rows"] = picked[:limit]
        if include_shadow:
            result["shadow_rows"] = shadow[:limit]
        return result

    def compare_strategies(self, dataset_id: str, path: str) -> dict:
        """
        同一字段在三种处置下的查询能力 vs 信息保真对比（不实际建三份数据，
        基于原始记录 + 规格规则推演，数字与真实入仓口径一致）。
        """
        latest = self.storage.latest_inference_run(dataset_id)
        if not latest:
            raise ValueError("缺少推断结果")
        path_item = latest["report"]["paths"].get(path)
        if not path_item:
            raise ValueError(f"路径不存在：{path}")

        raw_rows = self.storage.iter_raw_records(dataset_id)
        known = tuple(path_item["non_null_tags"])
        type_counts = path_item["type_counts"]
        counts = {"REJECT": dict(accepted=0, rejected=0),
                  "UNION": dict(accepted=0, rejected=0, variants=0),
                  "SHADOW": dict(accepted=0, shadowed=0, rejected=0)}
        total_non_null = 0
        # 三策略各自的有效承诺，与 build_spec_from_report 同口径
        promises = {
            name: effective_promise(type_counts, name)
            for name in ("REJECT", "UNION", "SHADOW")
        }

        for row in raw_rows:
            for p, v in typesys.flatten(row["record"]):
                if p != path:
                    continue
                actual = typesys.classify(v)
                if actual == typesys.NULL:
                    continue
                total_non_null += 1
                fits_reject = _fits_promise(v, actual, promises["REJECT"])
                fits_union = _fits_promise(v, actual, promises["UNION"])
                # REJECT
                if fits_reject:
                    counts["REJECT"]["accepted"] += 1
                else:
                    counts["REJECT"]["rejected"] += 1
                # UNION：全部保留，异类记为变体
                counts["UNION"]["accepted"] += 1
                if not fits_union:
                    counts["UNION"]["variants"] += 1
                # SHADOW
                if fits_reject:
                    counts["SHADOW"]["accepted"] += 1
                else:
                    counts["SHADOW"]["shadowed"] += 1

        return {
            "path": path,
            "total_non_null": total_non_null,
            "promised_tags": list(promises["REJECT"]),
            "known_tags": list(known),
            "strategies": _strategy_metrics(counts, total_non_null),
        }


def _fits_promise(value: Any, actual: str,
                  promised: Tuple[str, ...]) -> bool:
    try:
        coerce_value(value, actual, promised)
        return True
    except typesys.SchemaViolation:
        return False


def _strategy_metrics(counts: dict, total: int) -> dict:
    metrics = {}
    for name, c in counts.items():
        retained = total - c.get("rejected", 0)
        metrics[name] = {
            **c,
            "retention": retained / total if total else 1.0,
            "reject_ratio": c.get("rejected", 0) / total if total else 0.0,
            "typed_main_ratio": c["accepted"] / total if total else 1.0,
            "query_power": _query_power(name, c, total),
        }
    return metrics


def _query_power(name: str, c: dict, total: int) -> float:
    """
    0~1 的查询能力评分：主列上可直接做数值/比较聚合的覆盖比例。
    REJECT 主列全强类型但丢行；UNION 变体需要判别子分流；SHADOW
    主列强类型、影子另存。评分只统计主列强类型覆盖率。
    """
    if not total:
        return 1.0
    if name == "UNION":
        # 变体值不能直接参与数值聚合，按主列原生符合比例打折
        return c["accepted"] / total * 0.85
    return c["accepted"] / total


def _filter(rows: List[dict], op: str, value: Any) -> List[dict]:
    if op == "scan":
        return rows
    out = []
    for r in rows:
        cell = r["coerced"] if r["coerced"] is not None else r["value"]
        if op == "eq" and cell == value:
            out.append(r)
        elif op == "gt" and isinstance(cell, (int, float)) and cell > value:
            out.append(r)
        elif op == "lt" and isinstance(cell, (int, float)) and cell < value:
            out.append(r)
    return out


def _aggregate(rows: List[dict], op: str) -> Any:
    numeric = []
    for r in rows:
        cell = r["coerced"] if r["coerced"] is not None else r["value"]
        if isinstance(cell, bool) or not isinstance(cell, (int, float)):
            continue
        numeric.append(cell)
    if op == "count":
        return len(rows)
    if not numeric:
        return None
    if op == "avg":
        return sum(numeric) / len(numeric)
    if op == "min":
        return min(numeric)
    if op == "max":
        return max(numeric)
    return None


def _safe_json(text: Optional[str]) -> Any:
    if text is None:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return text
