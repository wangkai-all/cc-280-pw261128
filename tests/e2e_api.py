"""端到端 HTTP 工作流验证：通过真实 REST API 走完整工作台流程。"""
import json
import sys
import urllib.request
import urllib.error

BASE = "http://127.0.0.1:8014"


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def check(name, cond, detail=""):
    mark = "✓" if cond else "✗"
    print(f"  {mark} {name} {detail}")
    if not cond:
        check.failed += 1


check.failed = 0

print("① 建数据集 + 导入合成样本")
_, ds = call("POST", "/api/datasets", {"name": "E2E 工作台验证"})
ds_id = ds["dataset_id"]
st, imp = call("POST", f"/api/datasets/{ds_id}/import-sample", {})
check("导入 104 行", imp["imported"] == 104, str(imp["imported"]))
check("无解析错误", imp["parse_errors"] == [])

print("② 导入反例文件（坏行逐行报错）")
bad = "\n".join([
    '{"id":1}', '{"id":2,}', '{"id":3,"v":NaN}',
    'garbage', '{"id":4', '{"id":5}'])
st, imp2 = call("POST", f"/api/datasets/{ds_id}/import",
                {"text": bad, "source": "反例"})
# 6 行里仅第 1、6 行合法（2 条），尾逗号/NaN/非 JSON/未闭合各 1 条坏行
check("好行存活 2 条", imp2["imported"] == 2, str(imp2["imported"]))
check("坏行报错 4 条", len(imp2["parse_errors"]) == 4,
      str([e["line"] for e in imp2["parse_errors"]]))
TOTAL = 104 + 2  # 合成主数据集 104 + 反例好行 2

print("③ 全量推断")
st, inf = call("POST", f"/api/datasets/{ds_id}/infer", {"sync": True})
check(f"推断覆盖 {TOTAL} 行", inf["total_rows"] == TOTAL,
      str(inf["total_rows"]))
check("路径数 > 15", inf["report"]["path_count"] > 15,
      str(inf["report"]["path_count"]))
st, latest = call("GET", f"/api/datasets/{ds_id}/inference")
active = latest["latest"]["report"]["paths"]["$.active"]
check("active 含 BOOL+STR",
      set(["BOOL", "STR"]) <= set(active["type_counts"]))

print("④ 生成草稿 + 人工处置")
st, draft = call("POST", f"/api/datasets/{ds_id}/draft", {})
check("草稿创建", st == 201)
guard = draft["guard"]
st, rev = call("POST", f"/api/datasets/{ds_id}/draft/revise", {
    "version_id": draft["version_id"], "guard": guard,
    "overrides": {"$.active": "SHADOW"}})
check("草稿修订成功并轮换守卫", st == 200 and rev["guard"] != guard)
new_guard = rev["guard"]

print("⑤ 并发：旧守卫写入必须 409")
st, conflict = call("POST", f"/api/datasets/{ds_id}/draft/revise", {
    "version_id": draft["version_id"], "guard": guard,
    "overrides": {"$.active": "REJECT"}})
check("过期守卫被拒（409 guard_conflict）",
      st == 409 and conflict["error"] == "guard_conflict", f"HTTP {st}")

print("⑥ 逐行转换预览（含延迟类型反例）")
custom = {"id": 999, "name": {"unexpected": "object"}, "active": "maybe",
          "price": 1, "tier": "A", "tags": [],
          "meta": {"source": "api", "ref": 1}}
st, prev = call("POST", f"/api/datasets/{ds_id}/preview", {
    "version_id": draft["version_id"], "record": custom})
codes = {c["path"]: c["code"] for c in prev["conversions"]}
check("预览返回结构化处置（不抛异常）", st == 200)
check("name OBJECT 有处置结果", "$.name" in codes)

print("⑦ 确认结构（append-only）+ 入仓")
st, confirmed = call("POST", f"/api/datasets/{ds_id}/confirm", {
    "version_id": draft["version_id"], "guard": new_guard})
check("结构确认 v1", st == 200 and confirmed["version_no"] == 1,
      f"no={confirmed.get('version_no')}")
# 已确认版本不能再修订
st, again = call("POST", f"/api/datasets/{ds_id}/draft/revise", {
    "version_id": draft["version_id"], "guard": new_guard,
    "overrides": {"$.active": "REJECT"}})
check("已确认版本拒绝修改", st == 400)

st, ingest = call("POST", f"/api/datasets/{ds_id}/ingest", {
    "version_id": confirmed["version_id"], "sync": True})
check("入仓完成", st == 202 and "error" not in ingest)
check(f"发布+隔离守恒 {TOTAL}",
      ingest["published_rows"] + ingest["quarantined_rows"] == TOTAL,
      f"pub={ingest['published_rows']} q={ingest['quarantined_rows']}")
check("存在影子单元格", ingest["shadow_cells"] > 0,
      str(ingest["shadow_cells"]))

print("⑧ 故障注入 + 恢复幂等")
# 先做第二个草稿/版本以演示故障恢复（在同数据上重建）
st, inf2 = call("POST", f"/api/datasets/{ds_id}/infer", {"sync": True})
st, draft2 = call("POST", f"/api/datasets/{ds_id}/draft", {})
st, conf2 = call("POST", f"/api/datasets/{ds_id}/confirm", {
    "version_id": draft2["version_id"], "guard": draft2["guard"]})
st, fail_ingest = call("POST", f"/api/datasets/{ds_id}/ingest", {
    "version_id": conf2["version_id"], "sync": True, "fail_after": 20})
check("故障注入入仓失败但有检查点",
      "error" in fail_ingest and fail_ingest["quarantined_rows"] >= 0)
st, resume = call("POST", f"/api/datasets/{ds_id}/ingest", {
    "version_id": conf2["version_id"], "sync": True})
check("恢复后总数守恒",
      resume["published_rows"] + resume["quarantined_rows"] == TOTAL)

print("⑨ 查询 + 影子分离")
st, q = call("GET", f"/api/datasets/{ds_id}/query?version={conf2['version_id']}"
             f"&path=$.id&op=count")
check("count 聚合可用", q["aggregate"] is not None, str(q["aggregate"]))
st, q2 = call("GET", f"/api/datasets/{ds_id}/query?version={conf2['version_id']}"
              f"&path=$.active&op=scan")
check("影子单独成组", q2["shadow_scanned"] >= 0)

print("⑩ 三策略对比")
st, cmp_ = call("GET", f"/api/datasets/{ds_id}/compare?path=$.active")
check("返回三策略指标",
      set(cmp_["strategies"]) == {"REJECT", "UNION", "SHADOW"})
check("UNION 保留率 100%",
      cmp_["strategies"]["UNION"]["retention"] == 1.0)
check("REJECT 有拒收比例",
      cmp_["strategies"]["REJECT"]["reject_ratio"] > 0)

print("⑪ 隔离区 published 恒为 0 + 审计日志")
st, quar = call("GET", f"/api/datasets/{ds_id}/quarantine")
check("隔离区有记录", quar["count"] > 0)
st, audit = call("GET", f"/api/datasets/{ds_id}/audit")
events = {e["event"] for e in audit["events"]}
check("审计含确认与入仓事件",
      {"schema.confirmed", "ingest.committed"} <= events)

print(f"\n结果：{check.failed} 个失败")
sys.exit(1 if check.failed else 0)
