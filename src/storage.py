"""
storage.py —— SQLite 持久化层。

存储四类不可相互替代的事实：
1. 原始保留（raw_batches / raw_records）：导入即落盘，永不被转换覆盖。
2. 推断事实（inference_runs）：每次推断/持续接入的累加器快照，只追加。
3. 版本承诺（schema_versions）：草稿可改，已确认版本只追加、不可回退。
4. 入仓结果（cells / quarantine / jobs）：已发布单元格与隔离行严格分账。

并发控制：
- 进程内 threading.RLock 串行化写事务；SQLite 开 WAL。
- schema_versions.guard 是乐观并发令牌：更新/确认必须带回旧令牌，
  旧推断任务拿着过期令牌写草稿会被拒绝（CAS 语义），已确认结构永远
  不会被迟到的旧任务回退。
- 已确认版本 append-only：confirmed_seq 只增，没有 UPDATE/DELETE 路径。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    created_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_batches (
    batch_id     TEXT PRIMARY KEY,
    dataset_id   TEXT NOT NULL REFERENCES datasets(dataset_id),
    source_name  TEXT NOT NULL,
    format       TEXT NOT NULL,
    imported_at  REAL NOT NULL,
    record_count INTEGER NOT NULL,
    error_count  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_records (
    row_id       TEXT PRIMARY KEY,
    dataset_id   TEXT NOT NULL,
    batch_id     TEXT NOT NULL,
    row_index    INTEGER NOT NULL,
    raw_text     TEXT NOT NULL,           -- 原始 JSON 文本，永久保留
    imported_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_ds ON raw_records(dataset_id, row_index);

CREATE TABLE IF NOT EXISTS inference_runs (
    run_id       TEXT PRIMARY KEY,
    dataset_id   TEXT NOT NULL,
    job_id       TEXT,
    based_on_run TEXT,                     -- 持续接入时的父快照
    total_rows   INTEGER NOT NULL,
    accumulator  TEXT NOT NULL,           -- InferenceAccumulator JSON
    report       TEXT NOT NULL,
    created_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_inf_ds ON inference_runs(dataset_id, created_at);

CREATE TABLE IF NOT EXISTS schema_versions (
    version_id   TEXT PRIMARY KEY,
    dataset_id   TEXT NOT NULL,
    status       TEXT NOT NULL,           -- draft | confirmed | superseded
    version_no   INTEGER,                 -- 确认序号，草稿为 NULL
    based_on_run TEXT NOT NULL,
    spec         TEXT NOT NULL,           -- 每路径承诺+处置的完整规格
    guard        TEXT NOT NULL,           -- CAS 乐观令牌
    created_at   REAL NOT NULL,
    confirmed_at REAL
);
CREATE INDEX IF NOT EXISTS idx_ver_ds ON schema_versions(dataset_id, status);

CREATE TABLE IF NOT EXISTS jobs (
    job_id       TEXT PRIMARY KEY,
    dataset_id   TEXT NOT NULL,
    job_type     TEXT NOT NULL,           -- infer | ingest
    status       TEXT NOT NULL,           -- queued|running|committed|failed|cancelled
    checkpoint   TEXT NOT NULL DEFAULT '0',
    params       TEXT NOT NULL,
    error        TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_ds ON jobs(dataset_id, status);

CREATE TABLE IF NOT EXISTS cells (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id   TEXT NOT NULL,
    version_id   TEXT NOT NULL,
    row_id       TEXT NOT NULL,
    path         TEXT NOT NULL,
    is_shadow    INTEGER NOT NULL DEFAULT 0,
    value_type   TEXT NOT NULL,           -- 实际落库标签
    value_json   TEXT NOT NULL,           -- 原始值 JSON（联合/影子保真）
    coerced_json TEXT,                    -- 按承诺转换后的值（可空）
    UNIQUE(version_id, row_id, path, is_shadow)
);
CREATE INDEX IF NOT EXISTS idx_cells_lookup
    ON cells(dataset_id, version_id, path, is_shadow);

CREATE TABLE IF NOT EXISTS quarantine (
    q_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id   TEXT NOT NULL,
    version_id   TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    row_id       TEXT NOT NULL,
    path         TEXT NOT NULL,
    actual_type  TEXT NOT NULL,
    promised     TEXT NOT NULL,
    reason       TEXT NOT NULL,
    raw_value    TEXT NOT NULL,
    created_at   REAL NOT NULL,
    published    INTEGER NOT NULL DEFAULT 0  -- 隔离行永不自动发布
);
CREATE INDEX IF NOT EXISTS idx_q_ds ON quarantine(dataset_id, version_id);

CREATE TABLE IF NOT EXISTS audit_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id   TEXT NOT NULL,
    event        TEXT NOT NULL,
    detail       TEXT NOT NULL,
    created_at   REAL NOT NULL
);
"""


def now_ts() -> float:
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def compute_guard(payload: Any) -> str:
    """对规格/累加器等结构计算稳定令牌，用于乐观并发控制。"""
    canon = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:24]


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # ---- 审计 -------------------------------------------------------------

    def audit(self, dataset_id: str, event: str, detail: Any) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO audit_log(dataset_id, event, detail, created_at) "
                "VALUES (?,?,?,?)",
                (dataset_id, event, json.dumps(detail, ensure_ascii=False),
                 now_ts()),
            )

    # ---- 数据集 / 批次 / 原始记录 -----------------------------------------

    def create_dataset(self, name: str) -> str:
        ds_id = new_id("ds")
        with self._lock:
            self.conn.execute(
                "INSERT INTO datasets(dataset_id, name, created_at) VALUES (?,?,?)",
                (ds_id, name, now_ts()),
            )
            self.audit(ds_id, "dataset.created", {"name": name})
        return ds_id

    def list_datasets(self) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM datasets ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_dataset(self, dataset_id: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM datasets WHERE dataset_id=?", (dataset_id,)
            ).fetchone()
        return dict(row) if row else None

    def add_batch(self, dataset_id: str, source_name: str, fmt: str,
                  records: List[Any], raw_lines: List[str],
                  error_count: int) -> str:
        batch_id = new_id("bat")
        ts = now_ts()
        # 先在事务外取当前偏移量，避免事务内 SELECT 触发隐式提交
        start = self.count_raw_records(dataset_id)
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    "INSERT INTO raw_batches(batch_id, dataset_id, source_name,"
                    " format, imported_at, record_count, error_count) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (batch_id, dataset_id, source_name, fmt, ts,
                     len(records), error_count),
                )
                for i, (rec, raw) in enumerate(zip(records, raw_lines)):
                    row_id = new_id("row")
                    self.conn.execute(
                        "INSERT INTO raw_records(row_id, dataset_id, batch_id,"
                        " row_index, raw_text, imported_at) VALUES (?,?,?,?,?,?)",
                        (row_id, dataset_id, batch_id, start + i, raw, ts),
                    )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.audit(dataset_id, "batch.imported", {
                "batch_id": batch_id, "source": source_name,
                "records": len(records), "parse_errors": error_count,
            })
        return batch_id

    def count_raw_records(self, dataset_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) c FROM raw_records WHERE dataset_id=?",
            (dataset_id,),
        ).fetchone()
        return row["c"]

    def list_raw_records(self, dataset_id: str, limit: int = 500,
                         offset: int = 0) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT row_id, batch_id, row_index, raw_text FROM raw_records "
                "WHERE dataset_id=? ORDER BY row_index LIMIT ? OFFSET ?",
                (dataset_id, limit, offset),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["record"] = json.loads(d["raw_text"])
            d["raw_text"] = r["raw_text"]
            out.append(d)
        return out

    def iter_raw_records(self, dataset_id: str, after_index: int = 0
                         ) -> List[dict]:
        """取检查点之后的原始记录（持续接入/失败恢复都走这里）。"""
        with self._lock:
            rows = self.conn.execute(
                "SELECT row_id, row_index, raw_text FROM raw_records "
                "WHERE dataset_id=? AND row_index>=? ORDER BY row_index",
                (dataset_id, after_index),
            ).fetchall()
        return [
            {"row_id": r["row_id"], "row_index": r["row_index"],
             "record": json.loads(r["raw_text"])}
            for r in rows
        ]

    def list_batches(self, dataset_id: str) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM raw_batches WHERE dataset_id=? ORDER BY imported_at",
                (dataset_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- 推断快照（只追加）-------------------------------------------------

    def save_inference_run(self, dataset_id: str, job_id: Optional[str],
                           based_on_run: Optional[str], total_rows: int,
                           accumulator: dict, report: dict) -> str:
        run_id = new_id("inf")
        with self._lock:
            self.conn.execute(
                "INSERT INTO inference_runs(run_id, dataset_id, job_id,"
                " based_on_run, total_rows, accumulator, report, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (run_id, dataset_id, job_id, based_on_run, total_rows,
                 json.dumps(accumulator, ensure_ascii=False),
                 json.dumps(report, ensure_ascii=False), now_ts()),
            )
        return run_id

    def latest_inference_run(self, dataset_id: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM inference_runs WHERE dataset_id=? "
                "ORDER BY created_at DESC LIMIT 1",
                (dataset_id,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["accumulator"] = json.loads(d["accumulator"])
        d["report"] = json.loads(d["report"])
        return d

    # ---- 版本承诺：草稿 CAS + 已确认 append-only ---------------------------

    def create_draft(self, dataset_id: str, based_on_run: str,
                     spec: dict) -> dict:
        """每个数据集至多一个草稿；已存在草稿则报错（先处置/确认/作废）。"""
        with self._lock:
            existing = self.conn.execute(
                "SELECT version_id FROM schema_versions "
                "WHERE dataset_id=? AND status='draft'",
                (dataset_id,),
            ).fetchone()
            if existing:
                raise ValueError("已存在草稿版本，请先确认或作废它")
            version_id = new_id("ver")
            guard = compute_guard(spec)
            self.conn.execute(
                "INSERT INTO schema_versions(version_id, dataset_id, status,"
                " version_no, based_on_run, spec, guard, created_at) "
                "VALUES (?,?, 'draft', NULL, ?, ?, ?, ?)",
                (version_id, dataset_id, based_on_run,
                 json.dumps(spec, ensure_ascii=False), guard, now_ts()),
            )
            self.audit(dataset_id, "schema.draft_created",
                       {"version_id": version_id, "guard": guard})
            return self.get_version(version_id)

    def get_version(self, version_id: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM schema_versions WHERE version_id=?",
                (version_id,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["spec"] = json.loads(d["spec"])
        return d

    def get_draft(self, dataset_id: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM schema_versions WHERE dataset_id=? "
                "AND status='draft' ORDER BY created_at DESC LIMIT 1",
                (dataset_id,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["spec"] = json.loads(d["spec"])
        return d

    def latest_confirmed(self, dataset_id: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM schema_versions WHERE dataset_id=? "
                "AND status='confirmed' ORDER BY version_no DESC LIMIT 1",
                (dataset_id,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["spec"] = json.loads(d["spec"])
        return d

    def list_versions(self, dataset_id: str) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT version_id, status, version_no, based_on_run, guard,"
                " created_at, confirmed_at FROM schema_versions "
                "WHERE dataset_id=? ORDER BY created_at",
                (dataset_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_draft(self, version_id: str, spec: dict,
                     expected_guard: str) -> dict:
        """
        CAS 更新草稿。expected_guard 不匹配 -> 并发冲突，调用方必须处理。
        已确认版本永远走不到这里（WHERE status='draft'）。
        """
        with self._lock:
            row = self.conn.execute(
                "SELECT guard, dataset_id FROM schema_versions "
                "WHERE version_id=? AND status='draft'",
                (version_id,),
            ).fetchone()
            if not row:
                raise ValueError("草稿不存在或已被确认/作废，不能更新")
            if row["guard"] != expected_guard:
                raise GuardConflict(
                    f"草稿 {version_id} 已被其他任务修改（守卫不匹配）"
                )
            new_guard = compute_guard(spec)
            self.conn.execute(
                "UPDATE schema_versions SET spec=?, guard=?, created_at=? "
                "WHERE version_id=?",
                (json.dumps(spec, ensure_ascii=False), new_guard,
                 now_ts(), version_id),
            )
            self.audit(row["dataset_id"], "schema.draft_updated",
                       {"version_id": version_id, "new_guard": new_guard})
            return self.get_version(version_id)

    def discard_draft(self, version_id: str) -> None:
        with self._lock:
            self.conn.execute(
                "UPDATE schema_versions SET status='cancelled' "
                "WHERE version_id=? AND status='draft'",
                (version_id,),
            )

    def confirm_version(self, version_id: str,
                        expected_guard: str) -> dict:
        """
        确认结构：草稿 -> confirmed，分配单调递增 version_no。
        旧任务持过期守卫确认会失败；已确认的版本没有任何修改入口。
        """
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT * FROM schema_versions WHERE version_id=? "
                    "AND status='draft'",
                    (version_id,),
                ).fetchone()
                if not row:
                    raise ValueError("版本不是草稿，不能确认")
                if row["guard"] != expected_guard:
                    raise GuardConflict("守卫不匹配，结构已被并发修改")
                max_no = self.conn.execute(
                    "SELECT COALESCE(MAX(version_no), 0) m FROM schema_versions "
                    "WHERE dataset_id=? AND status='confirmed'",
                    (row["dataset_id"],),
                ).fetchone()["m"]
                next_no = max_no + 1
                ts = now_ts()
                self.conn.execute(
                    "UPDATE schema_versions SET status='confirmed', "
                    "version_no=?, confirmed_at=? WHERE version_id=?",
                    (next_no, ts, version_id),
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.audit(row["dataset_id"], "schema.confirmed", {
                "version_id": version_id, "version_no": next_no,
            })
            return self.get_version(version_id)

    # ---- 任务与检查点 ------------------------------------------------------

    def create_job(self, dataset_id: str, job_type: str,
                   params: dict) -> str:
        job_id = new_id("job")
        with self._lock:
            self.conn.execute(
                "INSERT INTO jobs(job_id, dataset_id, job_type, status,"
                " checkpoint, params, created_at, updated_at) "
                "VALUES (?,?,?, 'queued', '0', ?, ?, ?)",
                (job_id, dataset_id, job_type,
                 json.dumps(params, ensure_ascii=False), now_ts(), now_ts()),
            )
        return job_id

    def get_job(self, job_id: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["params"] = json.loads(d["params"])
        return d

    def list_jobs(self, dataset_id: str) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT job_id, job_type, status, checkpoint, error,"
                " created_at, updated_at FROM jobs WHERE dataset_id=? "
                "ORDER BY created_at DESC",
                (dataset_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_job(self, job_id: str, status: Optional[str] = None,
                   checkpoint: Optional[int] = None,
                   error: Optional[str] = None) -> None:
        with self._lock:
            sets, args = ["updated_at=?"], [now_ts()]
            if status is not None:
                sets.append("status=?")
                args.append(status)
            if checkpoint is not None:
                sets.append("checkpoint=?")
                args.append(str(checkpoint))
            if error is not None:
                sets.append("error=?")
                args.append(error)
            args.append(job_id)
            self.conn.execute(
                f"UPDATE jobs SET {', '.join(sets)} WHERE job_id=?", args
            )

    # ---- 单元格 / 隔离区（幂等写入，支持检查点恢复）------------------------

    def upsert_cell(self, dataset_id: str, version_id: str, row_id: str,
                    path: str, is_shadow: bool, value_type: str,
                    value_json: str, coerced_json: Optional[str]) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO cells(dataset_id, version_id, row_id, path,"
                " is_shadow, value_type, value_json, coerced_json) "
                "VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(version_id, row_id, path, is_shadow) DO UPDATE SET "
                "value_type=excluded.value_type, value_json=excluded.value_json,"
                " coerced_json=excluded.coerced_json",
                (dataset_id, version_id, row_id, path, int(is_shadow),
                 value_type, value_json, coerced_json),
            )

    def add_quarantine(self, dataset_id: str, version_id: str, job_id: str,
                       row_id: str, path: str, actual_type: str,
                       promised: List[str], reason: str,
                       raw_value: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO quarantine(dataset_id, version_id, job_id, row_id,"
                " path, actual_type, promised, reason, raw_value, created_at,"
                " published) VALUES (?,?,?,?,?,?,?,?,?,?,0)",
                (dataset_id, version_id, job_id, row_id, path, actual_type,
                 json.dumps(promised, ensure_ascii=False), reason, raw_value,
                 now_ts()),
            )

    def count_quarantine(self, dataset_id: str,
                         version_id: Optional[str] = None) -> int:
        with self._lock:
            if version_id:
                row = self.conn.execute(
                    "SELECT COUNT(*) c FROM quarantine "
                    "WHERE dataset_id=? AND version_id=?",
                    (dataset_id, version_id),
                ).fetchone()
            else:
                row = self.conn.execute(
                    "SELECT COUNT(*) c FROM quarantine WHERE dataset_id=?",
                    (dataset_id,),
                ).fetchone()
        return row["c"]

    def list_quarantine(self, dataset_id: str, version_id: Optional[str] = None,
                        limit: int = 200) -> List[dict]:
        with self._lock:
            if version_id:
                rows = self.conn.execute(
                    "SELECT * FROM quarantine WHERE dataset_id=? AND version_id=? "
                    "ORDER BY q_id LIMIT ?", (dataset_id, version_id, limit),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM quarantine WHERE dataset_id=? "
                    "ORDER BY q_id LIMIT ?", (dataset_id, limit),
                ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["promised"] = json.loads(d["promised"])
            d["raw_value_parsed"] = _safe_json(d["raw_value"])
            out.append(d)
        return out

    def quarantine_row_ids(self, version_id: str) -> set:
        """该版本下被拒收过的 row_id（恢复时用于避免重复记隔离账）。"""
        with self._lock:
            rows = self.conn.execute(
                "SELECT DISTINCT row_id FROM quarantine WHERE version_id=?",
                (version_id,),
            ).fetchall()
        return {r["row_id"] for r in rows}

    def count_cells(self, version_id: str, is_shadow: Optional[bool] = None
                    ) -> int:
        with self._lock:
            if is_shadow is None:
                row = self.conn.execute(
                    "SELECT COUNT(*) c FROM cells WHERE version_id=?",
                    (version_id,),
                ).fetchone()
            else:
                row = self.conn.execute(
                    "SELECT COUNT(*) c FROM cells WHERE version_id=? AND is_shadow=?",
                    (version_id, int(is_shadow)),
                ).fetchone()
        return row["c"]

    def list_audit(self, dataset_id: str, limit: int = 100) -> List[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM audit_log WHERE dataset_id=? ORDER BY id DESC LIMIT ?",
                (dataset_id, limit),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["detail"] = json.loads(d["detail"])
            out.append(d)
        return out


class GuardConflict(Exception):
    """CAS 守卫冲突：结构被并发任务抢先修改。"""


def _safe_json(text: str) -> Any:
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return text
