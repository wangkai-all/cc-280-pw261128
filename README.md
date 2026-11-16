# 类型推断与入仓工作台

原始 JSON 同一字段可能出现**数值、数值字符串、对象和缺失**。本工作台在入仓时给下游
一个**可解释的类型承诺**：采样没看到的类型后面出现时不静默截断，也不靠「全部转文本」
假装兼容。纯 Python 标准库 + SQLite + 原生 JS 实现，无第三方依赖。

## 运行

```bash
./run.sh                       # 默认 data/workbench.db:8000
# 或
python3 -m src.app data/wb.db 8000
# 浏览器打开 http://127.0.0.1:8000
```

验证：

```bash
python3 -I tests/test_workbench.py   # 34 个单元/集成测试
python3 -I tests/e2e_api.py          # 端到端 HTTP 工作流（需先启动服务器）
```

## 七个标签页对应完整工作流

1. **导入与原始保留**：JSON 数组 / JSONL / 单对象自动识别；坏行逐行报错（尾逗号、
   NaN/Infinity、溢出指数、非 JSON、未闭合），好行存活；`raw_records.raw_text`
   永久保留原文，任何转换都不覆盖它。
2. **类型推断**：流式累加，8 个类型标签（`NULL/BOOL/INT/FLOAT/NUMSTR/STR/ARRAY/OBJECT`），
   每个出现过的类型都计数；支持故障注入与检查点续跑。
3. **处置选择**：每个冲突字段可选 `REJECT / UNION / SHADOW`，人工可覆盖系统建议。
4. **逐行转换**：预览即实际入仓逻辑，逐路径展示原始值、承诺、处置码、主列/影子列结果。
5. **发布与并发**：草稿守卫 CAS（过期写入返回 409）；确认后版本 append-only；
   入仓检查点 + 幂等 UPSERT；并发演示按钮可复现旧任务被拦截。
6. **查询与对比**：主列扫描/比较/聚合，影子单元格独立成组绝不混入聚合；
   三策略的信息保留率 × 查询能力量化对比。
7. **审计与隔离**：隔离区 `published` 恒为 0，原值可回溯；全部结构/任务事件留痕。

## 三种处置的适用条件（取舍显式化）

| 策略 | 适用条件 | 查询能力 | 信息保真 |
|---|---|---|---|
| **REJECT 严格拒收** | 单一类型家族，或异类极稀有且语义可疑；下游要强类型可优化 | 最强，主列全强类型 | 拒收行进隔离区，原值不丢但不参与主分析 |
| **UNION 保留联合** | 多类型都是合法业务语义、势均力敌；下游能处理变体 | 聚合需按 `__kind__` 判别子分流（评分打折） | 零损失 |
| **SHADOW 影子新列** | 一个主导类型 + 少量异类；既要主分析强类型又要异类可回溯 | 主列强类型覆盖高 | 异类原值进影子列，零损失 |

关键边界：
- **bool 不并入 int**（Python 历史陷阱显式拒绝）；
- **NUMSTR 单独标记**，与普通 STR 区分，可确定性重解析为 INT/FLOAT，属数值家族；
- INT/FLOAT 共存时承诺安全拓宽为 FLOAT；float→int 这类可能丢精度的转换被拒绝；
- **采样未见类型**即使在 UNION 下也拒绝静默接纳，进隔离区并要求新版本承接；
- 不存在「全转 STR」的兜底路径。

## 并发与失败恢复不变量

1. 已确认版本 `schema_versions` 只有 INSERT 与草稿→confirmed 的状态流转，
   没有任何 UPDATE/DELETE 已确认版本的代码路径，`version_no` 单调递增。
2. 草稿用 `guard`（规格内容的 SHA-256 令牌）做乐观并发控制：旧推断任务持过期
   守卫写草稿抛 `GuardConflict`（HTTP 409）；它的推断事实仍只追加进
   `inference_runs`，经 `InferenceAccumulator.merge` 汇合，不能回退结构。
3. 任务按 `row_index` 检查点推进；`cells` 用 UPSERT、`quarantine` 按
   (version, row) 去重，失败重放天然幂等。
4. 只有 `jobs.status=committed` 的产出视为已发布；`quarantine.published`
   恒为 0，失败恢复绝不会把隔离行当已发布数据。

## 目录结构

```
src/
  typesys.py    类型分类、路径展开、推断累加、联合结构与处置建议
  parserio.py   严格 JSON/JSONL 解析与逐行错误
  storage.py    SQLite(WAL)：原始保留/推断快照/版本/单元格/隔离/审计
  engine.py     转换规则、任务编排、守卫 CAS、检查点恢复、查询与策略对比
  synthdata.py  带 ground truth 的合成数据、反例与延迟类型流
  app.py        ThreadingHTTPServer REST API + 静态页面
web/            真实网页（原生 HTML/CSS/JS，无外部依赖，可离线）
tests/          34 个单元集成测试 + 端到端 HTTP 工作流脚本
```
