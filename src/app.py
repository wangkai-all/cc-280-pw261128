"""
app.py —— 纯标准库 HTTP 工作台（ThreadingHTTPServer + 静态页面）。

所有写操作走 Engine/Storage 的锁与 CAS；本层只做 JSON 编解码与路由。
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from . import parserio, synthdata, typesys
from .engine import Engine, preview_row
from .storage import GuardConflict, Storage

WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "web")

_MIME = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
}


class AppContext:
    def __init__(self, db_path: str):
        self.storage = Storage(db_path)
        self.engine = Engine(self.storage)
        self.lock = threading.RLock()


def make_handler(ctx: AppContext) -> type:

    class Handler(BaseHTTPRequestHandler):
        server_version = "TypeBench/1.0"

        def log_message(self, fmt, *args):  # 安静一点
            pass

        # ---- 基础工具 -----------------------------------------------------

        def _send(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            return json.loads(raw.decode("utf-8"))

        def _static(self, rel: str) -> None:
            rel = rel.lstrip("/") or "index.html"
            target = os.path.normpath(os.path.join(WEB_DIR, rel))
            if not target.startswith(WEB_DIR) or not os.path.isfile(target):
                self.send_error(404, "not found")
                return
            ext = os.path.splitext(target)[1]
            with open(target, "rb") as fh:
                body = fh.read()
            self.send_response(200)
            self.send_header("Content-Type",
                             _MIME.get(ext, "application/octet-stream"))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        # ---- 路由 ---------------------------------------------------------

        def do_GET(self) -> None:
            try:
                self._route_get()
            except GuardConflict as exc:
                self._send(409, {"error": "guard_conflict", "message": str(exc)})
            except ValueError as exc:
                self._send(400, {"error": "bad_request", "message": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": "server_error", "message": str(exc)})

        def do_POST(self) -> None:
            try:
                self._route_post()
            except GuardConflict as exc:
                self._send(409, {"error": "guard_conflict", "message": str(exc)})
            except ValueError as exc:
                self._send(400, {"error": "bad_request", "message": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": "server_error", "message": str(exc)})

        def _route_get(self) -> None:
            parsed = urlparse(self.path)
            path, qs = parsed.path, parse_qs(parsed.query)
            st, eng = ctx.storage, ctx.engine

            if path == "/api/health":
                return self._send(200, {"ok": True})
            if path == "/api/datasets":
                return self._send(200, {"datasets": st.list_datasets()})

            m = _match(path, "/api/datasets/{}/versions")
            if m:
                return self._send(200, {
                    "versions": st.list_versions(m[0]),
                    "draft": st.get_draft(m[0]),
                    "confirmed": st.latest_confirmed(m[0]),
                })
            m = _match(path, "/api/datasets/{}/raw")
            if m:
                limit = int(qs.get("limit", ["200"])[0])
                offset = int(qs.get("offset", ["0"])[0])
                return self._send(200, {
                    "total": st.count_raw_records(m[0]),
                    "records": st.list_raw_records(m[0], limit, offset),
                    "batches": st.list_batches(m[0]),
                })
            m = _match(path, "/api/datasets/{}/inference")
            if m:
                run = st.latest_inference_run(m[0])
                return self._send(200, {"latest": run})
            m = _match(path, "/api/datasets/{}/jobs")
            if m:
                return self._send(200, {"jobs": st.list_jobs(m[0])})
            m = _match(path, "/api/datasets/{}/quarantine")
            if m:
                version = qs.get("version", [None])[0]
                return self._send(200, {
                    "items": st.list_quarantine(m[0], version),
                    "count": st.count_quarantine(m[0], version),
                })
            m = _match(path, "/api/datasets/{}/audit")
            if m:
                return self._send(200, {"events": st.list_audit(m[0])})
            m = _match(path, "/api/datasets/{}/compare")
            if m:
                field = qs.get("path", [""])[0]
                return self._send(200,
                                  eng.compare_strategies(m[0], field))
            m = _match(path, "/api/datasets/{}/query")
            if m:
                version = qs["version"][0]
                field = qs["path"][0]
                op = qs.get("op", ["scan"])[0]
                raw_value = qs.get("value", [None])[0]
                value = _parse_query_value(raw_value)
                include_shadow = qs.get("shadow", ["1"])[0] != "0"
                return self._send(200, eng.query(
                    m[0], version, field, op, value, include_shadow))
            if path.startswith("/api/"):
                return self._send(404, {"error": "not_found"})
            return self._static(path)

        def _route_post(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            body = self._read_json()
            st, eng = ctx.storage, ctx.engine

            m = _match(path, "/api/datasets")
            if m:
                ds_id = st.create_dataset(body.get("name", "未命名数据集"))
                return self._send(201, {"dataset_id": ds_id})

            m = _match(path, "/api/datasets/{}/import")
            if m:
                return self._import(m[0], body)
            m = _match(path, "/api/datasets/{}/import-sample")
            if m:
                records, gt = synthdata.build_main_dataset()
                text = synthdata.to_jsonl(records)
                parsed = parserio.parse_input(text)
                batch_id = st.add_batch(
                    m[0], body.get("source", "合成主数据集"),
                    parsed.format_detected, parsed.records,
                    parsed.raw_lines, len(parsed.errors))
                return self._send(201, {
                    "batch_id": batch_id,
                    "imported": len(parsed.records),
                    "parse_errors": [e.__dict__ for e in parsed.errors],
                    "ground_truth": gt,
                })
            m = _match(path, "/api/datasets/{}/infer")
            if m:
                base = body.get("base_run_id")
                if body.get("sync"):
                    out = eng.run_inference_sync(m[0], base,
                                                 body.get("fail_after"))
                else:
                    job_id = eng.start_inference(m[0], base,
                                                 body.get("fail_after"))
                    out = {"job_id": job_id, "status": "queued"}
                return self._send(202, out)
            m = _match(path, "/api/datasets/{}/draft")
            if m:
                version = eng.draft_from_latest(m[0],
                                                body.get("overrides"))
                return self._send(201, version)
            m = _match(path, "/api/datasets/{}/draft/revise")
            if m:
                version = eng.revise_draft(body["version_id"],
                                           body["guard"],
                                           body.get("overrides", {}))
                return self._send(200, version)
            m = _match(path, "/api/datasets/{}/confirm")
            if m:
                version = eng.confirm(body["version_id"], body["guard"])
                return self._send(200, version)
            m = _match(path, "/api/datasets/{}/ingest")
            if m:
                if body.get("sync"):
                    out = eng.run_ingest_sync(m[0], body["version_id"],
                                              body.get("fail_after"))
                else:
                    job_id = eng.start_ingest(m[0], body["version_id"],
                                              body.get("fail_after"))
                    out = {"job_id": job_id, "status": "queued"}
                return self._send(202, out)
            m = _match(path, "/api/datasets/{}/preview")
            if m:
                version = st.get_version(body["version_id"])
                if not version:
                    raise ValueError("版本不存在")
                record = body.get("record")
                if record is None and body.get("row_index") is not None:
                    rows = st.list_raw_records(
                        m[0], limit=1, offset=int(body["row_index"]))
                    record = rows[0]["record"] if rows else None
                return self._send(200, {
                    "conversions": preview_row(record, version["spec"])
                })
            return self._send(404, {"error": "not_found"})

        # ---- 导入（含反例）------------------------------------------------

        def _import(self, dataset_id: str, body: dict) -> None:
            text = body.get("text", "")
            source = body.get("source", "手工导入")
            parsed = parserio.parse_input(text)
            batch_id = ctx.storage.add_batch(
                dataset_id, source, parsed.format_detected or "empty",
                parsed.records, parsed.raw_lines, len(parsed.errors))
            self._send(201, {
                "batch_id": batch_id,
                "format": parsed.format_detected,
                "imported": len(parsed.records),
                "parse_errors": [
                    {"line": e.line, "snippet": e.snippet,
                     "message": e.message}
                    for e in parsed.errors
                ],
            })

    return Handler


def _match(path: str, pattern: str) -> Optional[Tuple[str, ...]]:
    """匹配成功返回占位符元组（零占位符时返回 (True,) 保持真值）。"""
    pp, tp = path.strip("/").split("/"), pattern.strip("/").split("/")
    if pp == [""]:
        pp = []
    if tp == [""]:
        tp = []
    if len(pp) != len(tp):
        return None
    out = []
    for a, b in zip(pp, tp):
        if b == "{}":
            out.append(a)
        elif a != b:
            return None
    return tuple(out) if out else (True,)


def _parse_query_value(raw: Optional[str]) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return raw


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8000) -> None:
    ctx = AppContext(db_path)
    httpd = ThreadingHTTPServer((host, port), make_handler(ctx))
    print(f"工作台运行中: http://{host}:{port}  (db={db_path})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        eng = ctx.engine
        eng.shutdown()
        ctx.storage.close()


if __name__ == "__main__":
    import sys

    db = sys.argv[1] if len(sys.argv) > 1 else "/workspace/data/workbench.db"
    p = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
    serve(db, port=p)
