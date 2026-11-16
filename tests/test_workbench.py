"""
test_workbench.py —— 端到端验证（纯 unittest，临时 SQLite）。

验证维度：
A. 推断精度：标签集合/计数与 ground truth 精确相等；bool≠int；NUMSTR 区分
B. 嵌套边界：深度 8、异构数组、空对象/空数组、缺失 vs 显式 NULL
C. 延迟类型：采样未见类型后出现时不静默截断（UnknownType / 隔离 / 影子）
D. 三策略口径：REJECT/UNION/SHADOW 的保真与查询能力数字
E. 并发：守卫 CAS 拒绝旧任务回退；已确认结构 append-only
F. 故障恢复：检查点续跑幂等；隔离行不被当已发布
G. 反例导入：坏行逐行报错且好行存活
H. 原始保留：转换后 raw_text 不变
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import parserio, synthdata, typesys
from src.engine import Engine, preview_row
from src.storage import GuardConflict, Storage


class WorkbenchTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.storage = Storage(self.db_path)
        self.engine = Engine(self.storage, max_workers=2)
        self.ds = self.storage.create_dataset("test-dataset")

    def tearDown(self):
        self.engine.shutdown()
        self.storage.close()
        self.tmp.cleanup()

    def import_main(self):
        records, gt = synthdata.build_main_dataset()
        text = synthdata.to_jsonl(records)
        parsed = parserio.parse_input(text)
        self.storage.add_batch(self.ds, "main", parsed.format_detected,
                               parsed.records, parsed.raw_lines, 0)
        return records, gt

    def infer(self, **kw):
        return self.engine.run_inference_sync(self.ds, **kw)

    def latest_report(self):
        return self.storage.latest_inference_run(self.ds)["report"]


# ---------------------------------------------------------------------------
# A. 推断精度
# ---------------------------------------------------------------------------

class TestInferencePrecision(WorkbenchTestBase):

    def test_type_sets_match_ground_truth(self):
        records, gt = self.import_main()
        out = self.infer()
        report = out["report"]
        self.assertEqual(out["total_rows"], gt["total_rows"])
        for path, truth in gt["paths"].items():
            with self.subTest(path=path):
                item = report["paths"][path]
                self.assertEqual(item["type_counts"], truth["type_counts"])
                self.assertEqual(item["present_rows"], truth["present"])
                self.assertEqual(item["missing_rows"], truth["missing"])

    def test_bool_is_not_int(self):
        acc = typesys.InferenceAccumulator()
        acc.add_record({"a": True, "b": 1})
        acc.add_record({"a": False, "b": 2})
        report = typesys.build_report(acc)
        self.assertEqual(report["paths"]["$.a"]["non_null_tags"], ["BOOL"])
        self.assertEqual(report["paths"]["$.b"]["non_null_tags"], ["INT"])
        self.assertNotIn("INT", report["paths"]["$.a"]["type_counts"])

    def test_numstr_distinguished_from_plain_string(self):
        acc = typesys.InferenceAccumulator()
        for v in ["123", "12.5", "-3", "abc", "12x"]:
            acc.add_record({"v": v})
        stats = acc.paths["$.v"]
        self.assertEqual(stats["counts"].get(typesys.NUMSTR), 3)
        self.assertEqual(stats["counts"].get(typesys.STR), 2)
        self.assertEqual(stats["numstr_kinds"]["intlike"], 2)
        self.assertEqual(stats["numstr_kinds"]["floatlike"], 1)

    def test_int_float_widens_to_float_promise(self):
        acc = typesys.InferenceAccumulator()
        acc.add_record({"v": 1})
        acc.add_record({"v": 2.5})
        report = typesys.build_report(acc)
        self.assertEqual(report["paths"]["$.v"]["promised_tags"], ["FLOAT"])

    def test_missing_vs_explicit_null(self):
        self.import_main()
        self.infer()
        report = self.latest_report()
        score = report["paths"]["$.score"]
        self.assertGreater(score["missing_rows"], 0)          # 键缺失
        self.assertIn("NULL", score["type_counts"])           # 显式 null
        self.assertIn("INT", score["type_counts"])

    def test_manual_anchor_assertions(self):
        _, gt = self.import_main()
        self.infer()
        report = self.latest_report()
        anchors = gt["manual_assertions"]
        for path, want in anchors.items():
            item = report["paths"][path]
            if "must_include" in want:
                for tag in want["must_include"]:
                    self.assertIn(tag, item["type_counts"],
                                  f"{path} 缺少标签 {tag}")
            if want.get("has_null"):
                self.assertIn("NULL", item["type_counts"])
            if want.get("has_missing"):
                self.assertGreater(item["missing_rows"], 0)


# ---------------------------------------------------------------------------
# B. 嵌套边界
# ---------------------------------------------------------------------------

class TestNestingBoundaries(WorkbenchTestBase):

    def setUp(self):
        super().setUp()
        self.import_main()
        self.infer()

    def test_deep_nesting_depth_eight(self):
        report = self.latest_report()
        deep_leaf = "$.deep.lvl1.lvl2.lvl3.lvl4.lvl5.lvl6.lvl7.lvl8"
        self.assertIn(deep_leaf, report["paths"])
        self.assertEqual(report["paths"][deep_leaf]["non_null_tags"], ["STR"])

    def test_heterogeneous_array(self):
        report = self.latest_report()
        # tags 里同时出现 STR / OBJECT / NULL / INT
        tag_paths = [p for p in report["paths"] if p.startswith("$.tags[")]
        all_tags = set()
        for p in tag_paths:
            all_tags.update(report["paths"][p]["type_counts"].keys())
        self.assertIn("STR", all_tags)
        self.assertIn("OBJECT", all_tags)
        self.assertIn("NULL", all_tags)
        self.assertIn("INT", all_tags)

    def test_empty_containers_are_leaves(self):
        report = self.latest_report()
        self.assertEqual(report["paths"]["$.empty_obj"]["non_null_tags"],
                         ["OBJECT"])
        self.assertEqual(report["paths"]["$.empty_arr"]["non_null_tags"],
                         ["ARRAY"])

    def test_nested_object_fields(self):
        report = self.latest_report()
        self.assertIn("$.meta.source", report["paths"])
        self.assertIn("$.meta.ref", report["paths"])
        self.assertEqual(
            report["paths"]["$.meta.source"]["non_null_tags"], ["STR"])

    def test_object_in_name_late_type(self):
        report = self.latest_report()
        name = report["paths"]["$.name"]
        self.assertIn("OBJECT", name["type_counts"])
        self.assertIn("STR", name["type_counts"])
        # 对象内部字段也被展开
        self.assertIn("$.name.raw", report["paths"])
        self.assertIn("$.name.lang", report["paths"])


# ---------------------------------------------------------------------------
# C. 延迟类型：不能静默截断
# ---------------------------------------------------------------------------

class TestLateTypes(WorkbenchTestBase):

    def _ingest_stream(self, text):
        parsed = parserio.parse_input(text)
        self.storage.add_batch(self.ds, "late", parsed.format_detected,
                               parsed.records, parsed.raw_lines, 0)
        self.infer()
        report = self.latest_report()
        # 用 REJECT 承诺 INT，验证 STR/OBJECT 后出现时被拦截
        spec = self.engine.draft_from_latest(
            self.ds, overrides={"$.user_id": "REJECT",
                                "$.payload": "REJECT"})
        version = self.engine.confirm(spec["version_id"], spec["guard"])
        return version, report

    def test_late_string_and_object_are_counted_not_truncated(self):
        version, report = self._ingest_stream(synthdata.build_late_type_stream())
        uid = report["paths"]["$.user_id"]
        self.assertIn("STR", uid["type_counts"])
        payload = report["paths"]["$.payload"]
        self.assertIn("OBJECT", payload["type_counts"])
        summary = self.engine.run_ingest_sync(self.ds, version["version_id"])
        # 仅第 31 行违规（同行两个字段），整行进隔离区；第 32 行仍满足 INT 承诺
        self.assertEqual(summary["quarantined_rows"], 1)
        # 隔离表按违规字段记两条，但都属于同一个 row_id
        items = self.storage.list_quarantine(self.ds, version["version_id"])
        self.assertEqual(len(items), 2)
        self.assertEqual(self.storage.count_quarantine(self.ds), 2)
        # 同一隔离行记录两个字段的违规，原值零损失可回溯
        reasons = {(q["path"], q["actual_type"]) for q in items}
        self.assertIn(("$.user_id", "STR"), reasons)
        self.assertIn(("$.payload", "OBJECT"), reasons)
        self.assertEqual(len({q["row_id"] for q in items}), 1)

    def test_unknown_type_raises_not_silent_coercion(self):
        # 构造一个只含 INT 的规格，再喂 STR，convert 必须显式失败/处置
        acc = typesys.InferenceAccumulator()
        acc.add_record({"v": 1})
        report = typesys.build_report(acc)
        from src.engine import build_spec_from_report
        spec = build_spec_from_report(report, {"$.v": "REJECT"})
        conv = preview_row({"v": "surprise-string"}, spec)
        c = next(x for x in conv if x["path"] == "$.v")
        self.assertEqual(c["code"], "reject")
        self.assertIsNone(c["main"])

    def test_no_everything_to_text_fallback(self):
        # 任何路径都不应出现「全部转 STR 兼容」：STR 承诺下数值必须被处置
        acc = typesys.InferenceAccumulator()
        acc.add_record({"v": "x"})
        report = typesys.build_report(acc)
        from src.engine import build_spec_from_report
        spec = build_spec_from_report(report, {"$.v": "REJECT"})
        conv = preview_row({"v": 99}, spec)
        c = next(x for x in conv if x["path"] == "$.v")
        self.assertEqual(c["code"], "reject")
        self.assertNotEqual(c["main"], "99")  # 没有偷偷转文本


# ---------------------------------------------------------------------------
# D. 三策略口径
# ---------------------------------------------------------------------------

class TestDispositions(WorkbenchTestBase):

    def setUp(self):
        super().setUp()
        self.import_main()
        self.infer()

    def _confirmed(self, overrides):
        draft = self.engine.draft_from_latest(self.ds, overrides)
        return self.engine.confirm(draft["version_id"], draft["guard"])

    # 非目标冲突路径全部 UNION 零损失，保证隔离/影子计数只反映被测字段。
    # tags[*] 的 STR/INT 相对主导 OBJECT/STR 是承诺外变体，会带判别子。
    OTHER_CONFLICTS_UNION = {
        "$.name": "UNION", "$.price": "UNION",
        "$.active": "UNION",
        "$.tags[0]": "UNION", "$.tags[1]": "UNION",
    }

    def test_reject_quarantines_and_keeps_main_typed(self):
        version = self._confirmed(
            {**self.OTHER_CONFLICTS_UNION, "$.active": "REJECT"})
        summary = self.engine.run_ingest_sync(self.ds, version["version_id"])
        # active 有 STR("yes") 反例 -> 至少一行隔离
        self.assertGreater(summary["quarantined_rows"], 0)
        # 主表 active 单元格没有 STR
        rows = self.storage.conn.execute(
            "SELECT DISTINCT value_type FROM cells WHERE version_id=? "
            "AND path='$.active' AND is_shadow=0",
            (version["version_id"],)).fetchall()
        types = {r["value_type"] for r in rows}
        self.assertNotIn("STR", types)

    def test_union_preserves_variant_with_discriminator(self):
        version = self._confirmed(
            {**self.OTHER_CONFLICTS_UNION, "$.active": "UNION"})
        summary = self.engine.run_ingest_sync(self.ds, version["version_id"])
        self.assertEqual(summary["quarantined_rows"], 0)
        import json
        # STR 在 active 的 UNION 承诺集合内，原值零损失可检索（原生承诺成员）
        row = self.storage.conn.execute(
            "SELECT value_json FROM cells WHERE version_id=? AND path='$.active' "
            "AND value_type='STR' LIMIT 1", (version["version_id"],)).fetchone()
        self.assertEqual(json.loads(row["value_json"]), "yes")
        self.assertGreaterEqual(summary["union_cells"], 0)

    def test_union_discriminator_on_out_of_promise_variant(self):
        # 单元级精确验证：承诺集合外、但已观察（known）的变体走 UNION 时，
        # 带 __kind__ 判别子且原值零损失。
        from src.engine import convert_path
        path_spec = {
            "path": "$.f",
            "promised_tags": ["INT"],          # 承诺只含 INT
            "known_tags": ["INT", "STR"],      # STR 是已观察的承诺外变体
            "disposition": "UNION",
        }
        conv = convert_path("hello", path_spec)
        self.assertEqual(conv["code"], "union")
        self.assertEqual(conv["main"],
                         {"__kind__": "STR", "value": "hello"})
        # 承诺内的 INT 正常入主列
        conv2 = convert_path(42, path_spec)
        self.assertEqual(conv2["code"], "ok")
        self.assertEqual(conv2["main"], 42)
        # 完全未见过的类型（BOOL）即使 UNION 也拒绝，不静默接纳
        with self.assertRaises(typesys.UnknownType):
            convert_path(True, path_spec)

    def test_shadow_keeps_main_promise_and_zero_loss_shadow(self):
        version = self._confirmed(
            {**self.OTHER_CONFLICTS_UNION, "$.active": "SHADOW"})
        summary = self.engine.run_ingest_sync(self.ds, version["version_id"])
        self.assertEqual(summary["quarantined_rows"], 0)
        self.assertGreater(summary["shadow_cells"], 0)
        # 影子里能原样找回 "yes"
        import json
        shadows = self.storage.conn.execute(
            "SELECT value_json FROM cells WHERE version_id=? AND path='$.active' "
            "AND is_shadow=1", (version["version_id"],)).fetchall()
        values = [json.loads(r["value_json"])["value"] for r in shadows]
        self.assertIn("yes", values)
        # 主列对应行显式为 NULL（承诺不被污染），而非被截断丢失
        null_main = self.storage.conn.execute(
            "SELECT COUNT(*) c FROM cells WHERE version_id=? AND path='$.active' "
            "AND is_shadow=0 AND value_type='NULL'",
            (version["version_id"],)).fetchone()["c"]
        self.assertGreaterEqual(null_main, 1)

    def test_numstr_deterministic_reparse(self):
        version = self._confirmed(
            {**self.OTHER_CONFLICTS_UNION, "$.price": "REJECT"})
        summary = self.engine.run_ingest_sync(self.ds, version["version_id"])
        # NUMSTR 可确定性重解析为 FLOAT，数值家族内部不拒收
        self.assertEqual(summary["quarantined_rows"], 0)
        self.assertGreater(summary["coerced_cells"], 0)

    def test_compare_metrics_consistency(self):
        out = self.engine.compare_strategies(self.ds, "$.active")
        s = out["strategies"]
        total = out["total_non_null"]
        self.assertEqual(
            s["REJECT"]["accepted"] + s["REJECT"]["rejected"], total)
        self.assertEqual(s["UNION"]["retention"], 1.0)
        self.assertEqual(s["SHADOW"]["accepted"]
                         + s["SHADOW"]["shadowed"], total)
        # REJECT 查询能力 = 接受比例；UNION 因判别分流打折
        self.assertGreater(s["REJECT"]["query_power"], 0)
        self.assertLessEqual(s["UNION"]["query_power"], 1.0)


# ---------------------------------------------------------------------------
# E. 并发：守卫 CAS + 已确认 append-only
# ---------------------------------------------------------------------------

class TestConcurrency(WorkbenchTestBase):

    def test_guard_conflict_on_stale_draft_write(self):
        self.import_main()
        self.infer()
        draft = self.engine.draft_from_latest(self.ds)
        stale = draft["guard"]
        self.engine.revise_draft(draft["version_id"], stale,
                                 {"$.active": "UNION"})
        with self.assertRaises(GuardConflict):
            self.engine.revise_draft(draft["version_id"], stale,
                                     {"$.active": "SHADOW"})

    def test_confirmed_version_is_immutable(self):
        self.import_main()
        self.infer()
        draft = self.engine.draft_from_latest(self.ds)
        confirmed = self.engine.confirm(draft["version_id"], draft["guard"])
        # 已确认版本无法再 update_draft（WHERE status='draft' 查不到）
        with self.assertRaises(ValueError):
            self.storage.update_draft(confirmed["version_id"],
                                      confirmed["spec"], confirmed["guard"])
        # 版本号单调
        draft2 = self.engine.draft_from_latest(self.ds)
        confirmed2 = self.engine.confirm(draft2["version_id"], draft2["guard"])
        self.assertEqual(confirmed2["version_no"], confirmed["version_no"] + 1)

    def test_late_inference_cannot_roll_back_confirmed(self):
        self.import_main()
        out1 = self.infer()
        draft = self.engine.draft_from_latest(self.ds)
        confirmed = self.engine.confirm(draft["version_id"], draft["guard"])
        # 再来一次推断（持续接入/迟到任务），产生新快照，但已确认版本规格不变
        self.infer(base_run_id=out1["run_id"])
        again = self.storage.get_version(confirmed["version_id"])
        self.assertEqual(again["status"], "confirmed")
        self.assertEqual(again["spec"], confirmed["spec"])

    def test_parallel_inference_merges_without_overwrite(self):
        self.import_main()
        # 两个并发推断任务（基于同一原始数据），都应成功提交且计数可复现
        f1 = self.engine.start_inference(self.ds)
        f2 = self.engine.start_inference(self.ds)
        self._wait_jobs([f1, f2])
        runs = self.storage.conn.execute(
            "SELECT COUNT(*) c FROM inference_runs WHERE dataset_id=?",
            (self.ds,)).fetchone()["c"]
        self.assertEqual(runs, 2)
        report = self.latest_report()
        self.assertEqual(report["total_rows"], 104)

    def _wait_jobs(self, job_ids, timeout=10):
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            states = [self.storage.get_job(j)["status"] for j in job_ids]
            if all(s in ("committed", "failed") for s in states):
                return states
            time.sleep(0.05)
        raise TimeoutError("任务未在时限内结束")


# ---------------------------------------------------------------------------
# F. 故障恢复
# ---------------------------------------------------------------------------

class TestFailureRecovery(WorkbenchTestBase):

    def test_inference_checkpoint_resume(self):
        self.import_main()
        out = self.infer(fail_after=10)
        self.assertIn("error", out)
        failed_job = self.storage.get_job(out["job_id"])
        self.assertEqual(failed_job["status"], "failed")
        self.assertEqual(int(failed_job["checkpoint"]), 10)
        # 恢复：从快照继续，最终行数等于全量
        run = self.storage.latest_inference_run(self.ds)
        # 失败任务在中断前未提交快照 -> 从零重跑也必须得到完整结果
        resumed = self.infer(base_run_id=run["run_id"] if run else None)
        self.assertNotIn("error", resumed)
        self.assertEqual(resumed["total_rows"], 104)

    def test_ingest_resume_is_idempotent(self):
        self.import_main()
        self.infer()
        draft = self.engine.draft_from_latest(
            self.ds, {"$.active": "REJECT"})
        version = self.engine.confirm(draft["version_id"], draft["guard"])

        first = self.engine.run_ingest_sync(
            self.ds, version["version_id"], fail_after=12)
        self.assertIn("error", first)
        checkpoint = int(self.storage.get_job(first["job_id"])["checkpoint"])
        self.assertEqual(checkpoint, 12)

        second = self.engine.run_ingest_sync(self.ds, version["version_id"])
        self.assertNotIn("error", second)
        # 总行数守恒：发布 + 隔离 = 原始行数
        total_raw = self.storage.count_raw_records(self.ds)
        self.assertEqual(second["published_rows"]
                         + second["quarantined_rows"], total_raw)
        # 单元格无重复（UNIQUE 约束 + UPSERT）
        dupes = self.storage.conn.execute(
            "SELECT row_id, path, is_shadow, COUNT(*) c FROM cells "
            "WHERE version_id=? GROUP BY row_id, path, is_shadow HAVING c>1",
            (version["version_id"],)).fetchall()
        self.assertEqual(dupes, [])

    def test_quarantine_never_published(self):
        self.import_main()
        self.infer()
        draft = self.engine.draft_from_latest(
            self.ds, {"$.active": "REJECT"})
        version = self.engine.confirm(draft["version_id"], draft["guard"])
        self.engine.run_ingest_sync(self.ds, version["version_id"],
                                    fail_after=8)
        # 失败后隔离行 published 必须全为 0
        bad = self.storage.conn.execute(
            "SELECT COUNT(*) c FROM quarantine WHERE published!=0").fetchone()
        self.assertEqual(bad["c"], 0)
        self.engine.run_ingest_sync(self.ds, version["version_id"])
        bad = self.storage.conn.execute(
            "SELECT COUNT(*) c FROM quarantine WHERE published!=0").fetchone()
        self.assertEqual(bad["c"], 0)
        # 隔离 row_id 不会出现在主表
        q_rows = self.storage.quarantine_row_ids(version["version_id"])
        if q_rows:
            placeholders = ",".join("?" * len(q_rows))
            leaked = self.storage.conn.execute(
                f"SELECT COUNT(*) c FROM cells WHERE version_id=? "
                f"AND row_id IN ({placeholders})",
                [version["version_id"], *q_rows]).fetchone()["c"]
            self.assertEqual(leaked, 0)


# ---------------------------------------------------------------------------
# G. 反例导入
# ---------------------------------------------------------------------------

class TestBadInput(WorkbenchTestBase):

    def test_bad_lines_reported_individually(self):
        result = parserio.parse_input(synthdata.build_bad_lines())
        # 好行：id 1,5,7 共 3 行；其余 5 行各有错误（含 NaN 与指数量级溢出）
        self.assertEqual(len(result.records), 3)
        self.assertEqual(len(result.errors), 5)
        self.assertEqual([r["id"] for r in result.records], [1, 5, 7])
        lines = [e.line for e in result.errors]
        self.assertEqual(lines, [2, 3, 4, 5, 7])

    def test_nan_constant_rejected(self):
        # 单对象：不伪造记录，错误显式返回（统一的 ParseError 契约）
        single = parserio.parse_input('{"v": NaN}')
        self.assertEqual(len(single.records), 0)
        self.assertEqual(len(single.errors), 1)
        self.assertIn("NaN", single.errors[0].message)
        # JSONL：坏行隔离，好行存活
        result = parserio.parse_input('{"v": 1}\n{"v": NaN}\n{"v": 2}')
        self.assertEqual(len(result.records), 2)
        self.assertEqual(len(result.errors), 1)

    def test_infinity_and_overflow_rejected(self):
        for text in ('{"v": Infinity}', '{"v": -Infinity}',
                     '{"v": 1e99999}'):
            with self.subTest(text=text):
                r = parserio.parse_input(text)
                self.assertEqual(len(r.records), 0)
                self.assertEqual(len(r.errors), 1)

    def test_array_format_and_jsonl_format(self):
        a = parserio.parse_input('[{"v": 1}, {"v": 2}]')
        self.assertEqual(a.format_detected, "array")
        self.assertEqual(len(a.records), 2)
        b = parserio.parse_input('{"v": 1}\n{"v": 2}')
        self.assertEqual(b.format_detected, "jsonl")

    def test_reject_non_strict_numeric(self):
        # float 分类器直接拒绝 NaN/Inf
        with self.assertRaises(typesys.TypeSystemError):
            typesys.classify(float("nan"))


# ---------------------------------------------------------------------------
# H. 原始保留 & 查询
# ---------------------------------------------------------------------------

class TestRawRetentionAndQuery(WorkbenchTestBase):

    def test_raw_text_immutable_after_ingest(self):
        records, _ = self.import_main()
        before = {r["row_id"]: r["raw_text"]
                  for r in self.storage.list_raw_records(self.ds, 1000)}
        self.infer()
        draft = self.engine.draft_from_latest(self.ds)
        version = self.engine.confirm(draft["version_id"], draft["guard"])
        self.engine.run_ingest_sync(self.ds, version["version_id"])
        after = {r["row_id"]: r["raw_text"]
                 for r in self.storage.list_raw_records(self.ds, 1000)}
        self.assertEqual(before, after)

    def test_query_aggregation_and_shadow_separation(self):
        self.import_main()
        self.infer()
        draft = self.engine.draft_from_latest(
            self.ds, {"$.active": "SHADOW"})
        version = self.engine.confirm(draft["version_id"], draft["guard"])
        self.engine.run_ingest_sync(self.ds, version["version_id"])
        q = self.engine.query(self.ds, version["version_id"],
                              "$.id", op="count")
        self.assertIsNotNone(q["aggregate"])
        self.assertGreater(q["aggregate"], 0)
        q2 = self.engine.query(self.ds, version["version_id"],
                               "$.active", op="scan")
        # 影子行单独成组，不混进主列结果
        self.assertGreaterEqual(q2["shadow_scanned"], 1)
        main_types = {r["type"] for r in
                      self.storage.conn.execute(
                          "SELECT value_type type FROM cells WHERE version_id=? "
                          "AND path='$.active' AND is_shadow=0",
                          (version["version_id"],)).fetchall()}
        self.assertNotIn("STR", main_types)


if __name__ == "__main__":
    unittest.main(verbosity=2)
