/* app.js —— 类型推断与入仓工作台前端（原生 JS，无依赖） */
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));
const state = {
  datasetId: null,
  latestRun: null,
  draft: null,
  confirmed: null,
  draftGuard: null,
};

// ---------------------------------------------------------------------------
// 基础工具
// ---------------------------------------------------------------------------

async function api(path, opts = {}) {
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers: { "Content-Type": "application/json" },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const payload = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(payload.message || `HTTP ${res.status}`);
    err.status = res.status;
    err.code = payload.error;
    throw err;
  }
  return payload;
}

function toast(msg, isErr = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.className = "toast" + (isErr ? " err" : "");
  setTimeout(() => el.classList.add("hidden"), 3800);
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function tagsHtml(tags) {
  if (!tags || !tags.length) return '<span class="muted">（仅 NULL/缺失）</span>';
  return tags.map((t) => `<span class="tag tag-${t}">${t}</span>`).join("");
}

function statusHtml(s) {
  return `<span class="status status-${s}">${s}</span>`;
}

function pretty(v) {
  try {
    return JSON.stringify(v, null, 1);
  } catch {
    return String(v);
  }
}

function pct(x) {
  return (x * 100).toFixed(1) + "%";
}

// ---------------------------------------------------------------------------
// 标签页切换
// ---------------------------------------------------------------------------

$$("#tabs .tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    $$("#tabs .tab").forEach((t) => t.classList.remove("active"));
    $$(".panel").forEach((p) => p.classList.remove("active"));
    tab.classList.add("active");
    $("#panel-" + tab.dataset.tab).classList.add("active");
    refreshActiveTab(tab.dataset.tab);
  });
});

function refreshActiveTab(name) {
  if (!state.datasetId) return;
  if (name === "import") loadRaw();
  if (name === "infer") loadInference();
  if (name === "disposition") loadDraftMeta();
  if (name === "preview") loadVersionSelectors();
  if (name === "publish") { loadVersions(); loadJobs(); }
  if (name === "query") { loadVersionSelectors(); loadComparePaths(); }
  if (name === "audit") { loadQuarantine(); loadAudit(); }
}

// ---------------------------------------------------------------------------
// 数据集
// ---------------------------------------------------------------------------

async function loadDatasets(selectFirst = true) {
  const { datasets } = await api("/api/datasets");
  const sel = $("#dsSelect");
  sel.innerHTML = datasets.length
    ? datasets.map((d) => `<option value="${d.dataset_id}">${esc(d.name)} · ${d.dataset_id.slice(0, 10)}</option>`).join("")
    : '<option value="">（无数据集，请新建）</option>';
  if (selectFirst && datasets.length) {
    state.datasetId = sel.value;
  }
}

$("#btnNewDs").addEventListener("click", async () => {
  const name = prompt("新数据集名称", "演示数据集 " + new Date().toLocaleTimeString());
  if (!name) return;
  const { dataset_id } = await api("/api/datasets", { method: "POST", body: { name } });
  await loadDatasets(false);
  $("#dsSelect").value = dataset_id;
  state.datasetId = dataset_id;
  toast("已创建数据集 " + dataset_id);
  fullRefresh();
});

$("#dsSelect").addEventListener("change", () => {
  state.datasetId = $("#dsSelect").value;
  fullRefresh();
});

$("#btnRefresh").addEventListener("click", fullRefresh);

async function fullRefresh() {
  state.latestRun = null; state.draft = null; state.confirmed = null;
  await Promise.all([loadRaw(), loadInference(), loadDraftMeta(),
    loadVersions(), loadJobs(), loadVersionSelectors(), loadComparePaths(),
    loadQuarantine(), loadAudit()]);
}

const ds = () => {
  if (!state.datasetId) throw new Error("请先选择或新建数据集");
  return state.datasetId;
};

// ---------------------------------------------------------------------------
// ① 导入与原始保留
// ---------------------------------------------------------------------------

$("#btnLoadSample").addEventListener("click", async () => {
  const { synthdata } = await fetchSampleSource();
  $("#srcName").value = "合成主数据集（含延迟类型/嵌套边界）";
  $("#importText").value = synthdata;
});

$("#btnLoadLate").addEventListener("click", () => {
  // 延迟类型流在前端内联生成（与后端 synthdata.build_late_type_stream 同构）
  $("#srcName").value = "延迟类型流（前 30 行 INT，第 31 行突现 STR/OBJECT）";
  $("#importText").value = buildLateStreamFallback();
});

$("#btnLoadBad").addEventListener("click", () => {
  $("#srcName").value = "反例文件（坏行混合）";
  $("#importText").value = [
    '{"id": 1, "v": 1}',
    '{"id": 2, "v": 2,}',
    '{"id": 3, "v": NaN}',
    "not-json-at-all",
    '{"id": 4, "v": 4',
    '{"id": 5, "v": 5}',
  ].join("\n");
});

async function fetchSampleSource() {
  // 直接调用后端导入接口生成样本，再拉回原文展示
  const datasetId = ds();
  const imported = await api(`/api/datasets/${datasetId}/import-sample`,
    { method: "POST", body: { source: "合成主数据集" } });
  const raw = await api(`/api/datasets/${datasetId}/raw?limit=5`);
  const lines = raw.records.map((r) => JSON.stringify(r.record));
  await loadRaw();
  toast(`已导入合成数据集 ${imported.imported} 行`);
  return { synthdata: lines.join("\n"), imported };
}

function buildLateStreamFallback() {
  const lines = [];
  for (let i = 0; i < 30; i++) lines.push(JSON.stringify({ user_id: i, payload: i * 2 }));
  lines.push(JSON.stringify({ user_id: "A-31", payload: { k: "v" } }));
  lines.push(JSON.stringify({ user_id: 32, payload: 32 }));
  return lines.join("\n");
}

$("#btnImport").addEventListener("click", async () => {
  try {
    const out = await api(`/api/datasets/${ds()}/import`, {
      method: "POST",
      body: { text: $("#importText").value, source: $("#srcName").value || "手工导入" },
    });
    const box = $("#importResult");
    box.className = "result-box ok";
    box.innerHTML = `<div class="kv">成功导入 <b>${out.imported}</b> 条（格式：${esc(out.format)}），批次 ${esc(out.batch_id)}</div>`
      + (out.parse_errors.length
        ? `<div class="result-box err">${out.parse_errors.length} 条坏行被隔离记录（未伪造为空值）：`
          + out.parse_errors.map((e) =>
            `<div class="kv">第 ${e.line} 行：${esc(e.message)}<br><span class="muted mono">${esc(e.snippet)}</span></div>`).join("")
          + "</div>"
        : '<div class="muted">无解析错误</div>');
    await loadRaw();
  } catch (e) {
    $("#importResult").className = "result-box err";
    $("#importResult").textContent = "导入失败：" + e.message;
  }
});

async function loadRaw() {
  if (!state.datasetId) return;
  const data = await api(`/api/datasets/${state.datasetId}/raw?limit=100`);
  $("#batchList").innerHTML = data.batches.map((b) =>
    `<div class="meta-item"><span class="t">批次 ${esc(b.batch_id)}</span><br>
      来源：${esc(b.source_name)} · 格式 ${b.format} · <b>${b.record_count}</b> 行 ·
      解析错误 ${b.error_count} 条</div>`).join("") || '<span class="muted">暂无批次</span>';
  $("#rawTable tbody").innerHTML = data.records.map((r) =>
    `<tr><td>${r.row_index}</td><td class="mono">${esc(r.row_id.slice(0, 14))}…</td>
     <td class="mono" style="max-width:560px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(JSON.stringify(r.record))}</td></tr>`
  ).join("") || '<tr><td colspan="3" class="muted">暂无原始记录</td></tr>';
}

// ---------------------------------------------------------------------------
// ② 推断
// ---------------------------------------------------------------------------

$("#btnInfer").addEventListener("click", () => runInfer({ sync: true }));
$("#btnInferFail").addEventListener("click", () => runInfer({ sync: true, fail_after: 10 }));
$("#btnInferResume").addEventListener("click", async () => {
  // 恢复 = 以最新快照为基线继续推断（检查点之后的行）
  const run = state.latestRun;
  await runInfer({ sync: true, base_run_id: run ? run.run_id : undefined });
});

async function runInfer(body) {
  try {
    const out = await api(`/api/datasets/${ds()}/infer`, { method: "POST", body });
    if (out.error) {
      $("#inferSummary").className = "result-box err";
      $("#inferSummary").innerHTML = `任务失败（检查点已保留，可恢复）：<b>${esc(out.error)}</b>`;
    } else {
      $("#inferSummary").className = "result-box ok";
      $("#inferSummary").innerHTML = `<div class="kv">推断完成：累计 <b>${out.total_rows}</b> 行，${out.report.path_count} 个字段路径，快照 ${esc(out.run_id)}</div>`;
    }
    await loadInference();
    await loadJobs();
  } catch (e) {
    toast("推断失败：" + e.message, true);
  }
}

async function loadInference() {
  if (!state.datasetId) return;
  const { latest } = await api(`/api/datasets/${state.datasetId}/inference`);
  state.latestRun = latest;
  if (!latest) {
    $("#inferMeta").textContent = "尚无推断快照";
    $("#inferTable tbody").innerHTML = '<tr><td colspan="7" class="muted">执行推断后展示</td></tr>';
    return;
  }
  $("#inferMeta").textContent =
    `快照 ${latest.run_id.slice(0, 14)}… · 行数 ${latest.total_rows}`;
  const paths = Object.values(latest.report.paths);
  $("#inferTable tbody").innerHTML = paths.map((p) => {
    const counts = Object.entries(p.type_counts)
      .map(([t, n]) => `<span class="tag tag-${t}">${t} ×${n}</span>`).join(" ");
    const numstr = p.numstr_kinds.intlike + p.numstr_kinds.floatlike
      ? `intlike ${p.numstr_kinds.intlike} / floatlike ${p.numstr_kinds.floatlike}`
      : "—";
    const samples = Object.entries(p.samples)
      .map(([t, v]) => `<span class="tag tag-${t}">${esc(typeof v === "object" ? JSON.stringify(v) : String(v))}</span>`).join(" ");
    return `<tr>
      <td class="path">${esc(p.path)}</td>
      <td>${tagsHtml(p.promised_tags)}</td>
      <td>${counts}</td>
      <td>${p.missing_rows}（${pct(p.missing_ratio)}）</td>
      <td class="mono">${numstr}</td>
      <td style="max-width:260px">${samples}</td>
      <td><span class="chip ${p.advice.disposition.toLowerCase()}">${p.advice.disposition}</span>
          <div class="muted" style="font-size:11px;margin-top:4px">${esc(p.advice.reason)}</div></td>
    </tr>`;
  }).join("");
}

// ---------------------------------------------------------------------------
// ③ 处置选择
// ---------------------------------------------------------------------------

$("#btnDraft").addEventListener("click", async () => {
  try {
    const draft = await api(`/api/datasets/${ds()}/draft`, { method: "POST", body: {} });
    state.draft = draft;
    state.draftGuard = draft.guard;
    toast("草稿已生成：" + draft.version_id);
    await renderDraft(draft);
    await loadVersions();
  } catch (e) {
    toast(e.message, true);
  }
});

async function loadDraftMeta() {
  if (!state.datasetId) return;
  const data = await api(`/api/datasets/${state.datasetId}/versions`);
  state.draft = data.draft;
  state.confirmed = data.confirmed;
  if (data.draft) {
    state.draftGuard = data.draft.guard;
    await renderDraft(data.draft);
  } else {
    $("#draftMeta").textContent = "无草稿（基于最新推断生成）";
    $("#draftBox").innerHTML = "";
  }
}

async function renderDraft(draft) {
  $("#draftMeta").innerHTML = `草稿 <span class="mono">${esc(draft.version_id)}</span>
    · 守卫 <span class="mono">${esc(draft.guard.slice(0, 12))}…</span>`;
  const paths = Object.values(draft.spec.paths);
  const rows = paths.map((p, i) => `
    <tr>
      <td class="path">${esc(p.path)}</td>
      <td>${tagsHtml(p.promised_tags)}</td>
      <td>
        <select class="disp" data-path="${esc(p.path)}" data-idx="${i}">
          ${["REJECT", "UNION", "SHADOW"].map((d) =>
            `<option value="${d}" ${p.disposition === d ? "selected" : ""}>${d}</option>`).join("")}
        </select>
      </td>
      <td class="muted" style="font-size:11.5px">${esc(p.reason)}</td>
    </tr>`).join("");
  $("#draftBox").innerHTML = `<table class="data-table wide">
    <thead><tr><th>路径</th><th>承诺类型</th><th>处置策略（可人工覆盖建议）</th><th>推断建议理由</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
}

$("#btnSaveDraft").addEventListener("click", async () => {
  if (!state.draft) return toast("请先生成草稿", true);
  const overrides = {};
  $$("#draftBox select.disp").forEach((sel) => {
    overrides[sel.dataset.path] = sel.value;
  });
  try {
    const updated = await api(`/api/datasets/${ds()}/draft/revise`, {
      method: "POST",
      body: { version_id: state.draft.version_id, guard: state.draftGuard, overrides },
    });
    state.draft = updated;
    state.draftGuard = updated.guard;
    toast("草稿已保存，守卫已轮换：" + updated.guard.slice(0, 10));
  } catch (e) {
    if (e.status === 409) {
      toast("守卫冲突！结构已被并发任务修改，已拒绝本次回退写入（409）", true);
    } else {
      toast(e.message, true);
    }
    await loadDraftMeta();
  }
});

// ---------------------------------------------------------------------------
// ④ 逐行转换
// ---------------------------------------------------------------------------

async function loadVersionSelectors() {
  if (!state.datasetId) return;
  const { versions } = await api(`/api/datasets/${state.datasetId}/versions`);
  const opts = versions.filter((v) => v.status === "draft" || v.status === "confirmed")
    .map((v) => `<option value="${v.version_id}">${v.status}${v.version_no ? " v" + v.version_no : ""} · ${v.version_id.slice(0, 10)}</option>`)
    .join("");
  $("#previewVersion").innerHTML = opts;
  $("#queryVersion").innerHTML = opts;
}

$("#btnPreviewRow").addEventListener("click", async () => {
  const version_id = $("#previewVersion").value;
  if (!version_id) return toast("请先选择草稿/版本", true);
  const out = await api(`/api/datasets/${ds()}/preview`, {
    method: "POST",
    body: { version_id, row_index: Number($("#previewRow").value || 0) },
  });
  renderConversions(out.conversions);
});

$("#btnPreviewCustom").addEventListener("click", async () => {
  const version_id = $("#previewVersion").value;
  if (!version_id) return toast("请先选择草稿/版本", true);
  let record;
  try {
    record = JSON.parse($("#previewCustom").value);
  } catch (e) {
    return toast("自定义记录不是合法 JSON：" + e.message, true);
  }
  try {
    const out = await api(`/api/datasets/${ds()}/preview`, {
      method: "POST", body: { version_id, record },
    });
    renderConversions(out.conversions);
  } catch (e) {
    $("#previewResult").className = "result-box err";
    $("#previewResult").innerHTML = `<div class="kv">遇到采样未见类型，拒绝静默截断：<b>${esc(e.message)}</b></div>`;
  }
});

function renderConversions(conv) {
  const codeText = {
    ok: "符合承诺，入主列", coerced: "确定性转换后入主列",
    union: "联合变体（带判别子）", shadow: "异类原值进影子列",
    reject: "拒收 → 隔离区", null: "NULL/缺失",
  };
  $("#previewResult").innerHTML = conv.map((c) => `
    <div class="conv-card code-${c.code}">
      <div><span class="path">${esc(c.path)}</span>
        <span class="tag tag-${c.actual}">${c.actual}</span>
        → 承诺 ${c.promised.length ? tagsHtml(c.promised) : '<span class="muted">无</span>'}
        ${c.disposition ? `<span class="chip ${c.disposition.toLowerCase()}">${c.disposition}</span>` : ""}
        <b>[${codeText[c.code] || c.code}]</b>
      </div>
      ${c.reason ? `<div class="muted" style="margin-top:4px">${esc(c.reason)}</div>` : ""}
      <div style="margin-top:6px">
        <span class="muted">原始值：</span><pre class="json">${esc(pretty(c.raw))}</pre>
        ${c.main !== null && c.main !== undefined ? `<span class="muted">主列：</span><pre class="json">${esc(pretty(c.main))}</pre>` : ""}
        ${c.shadow ? `<span class="muted">影子列：</span><pre class="json">${esc(pretty(c.shadow))}</pre>` : ""}
      </div>
    </div>`).join("");
}

// ---------------------------------------------------------------------------
// ⑤ 发布与并发
// ---------------------------------------------------------------------------

$("#btnConfirm").addEventListener("click", async () => {
  if (!state.draft) return toast("没有可确认的草稿", true);
  try {
    const v = await api(`/api/datasets/${ds()}/confirm`, {
      method: "POST",
      body: { version_id: state.draft.version_id, guard: state.draftGuard },
    });
    toast(`结构已确认：version_no=${v.version_no}（append-only，不可回退）`);
    state.draft = null;
    await loadVersions();
    await loadVersionSelectors();
  } catch (e) {
    toast(e.status === 409 ? "守卫冲突，确认被拒绝：" + e.message : e.message, true);
  }
});

$("#btnIngest").addEventListener("click", () => runIngest({}));
$("#btnIngestFail").addEventListener("click", () => runIngest({ fail_after: 12 }));

async function runIngest(extra) {
  const version_id = state.confirmed ? state.confirmed.version_id
    : $("#queryVersion").value || $("#previewVersion").value;
  if (!version_id) return toast("请先确认结构再入仓", true);
  const out = await api(`/api/datasets/${ds()}/ingest`, {
    method: "POST", body: { version_id, sync: true, ...extra },
  });
  const box = $("#publishResult");
  if (out.error) {
    box.className = "result-box err";
    box.innerHTML = `<div class="kv">入仓中断：<b>${esc(out.error)}</b><br>
      已处理检查点保留，重跑同一任务将幂等续跑；隔离行不会被当作已发布。</div>`;
  } else {
    box.className = "result-box ok";
    box.innerHTML = `<div class="kv">
      已发布行 <b>${out.published_rows}</b> · 隔离行 <b>${out.quarantined_rows}</b> ·
      影子单元格 ${out.shadow_cells} · 联合变体 ${out.union_cells} · 确定性转换 ${out.coerced_cells}
      </div>`;
  }
  await loadJobs();
  await loadQuarantine();
}

$("#btnRaceDemo").addEventListener("click", async () => {
  // 模拟：旧任务拿着草稿创建时的旧守卫，在草稿被修订后仍尝试写入
  if (!state.draft) return toast("请先在③生成草稿", true);
  const staleGuard = state.draftGuard;
  const box = $("#raceResult");
  try {
    // 先用当前守卫做一次合法修订，轮换守卫
    await api(`/api/datasets/${ds()}/draft/revise`, {
      method: "POST",
      body: { version_id: state.draft.version_id, guard: staleGuard, overrides: {} },
    });
    // 再用旧守卫写入 —— 必须 409
    await api(`/api/datasets/${ds()}/draft/revise`, {
      method: "POST",
      body: { version_id: state.draft.version_id, guard: staleGuard,
              overrides: {} },
    });
    box.className = "result-box err";
    box.textContent = "严重问题：旧守卫写入竟然成功了";
  } catch (e) {
    if (e.status === 409) {
      box.className = "result-box ok";
      box.innerHTML = `<div class="kv">符合预期：旧推断任务持过期守卫写入被拒（HTTP 409 guard_conflict）。<br>
        已确认结构 append-only，迟到任务只能把推断事实追加到新快照并 merge，不能回退结构。</div>`;
      toast("并发演示：守卫冲突正确拦截");
    } else {
      box.className = "result-box err";
      box.textContent = e.message;
    }
  }
  await loadDraftMeta();
});

async function loadVersions() {
  if (!state.datasetId) return;
  const data = await api(`/api/datasets/${state.datasetId}/versions`);
  state.confirmed = data.confirmed;
  state.draft = data.draft;
  if (data.draft) state.draftGuard = data.draft.guard;
  $("#versionTimeline").innerHTML = data.versions.map((v) => `
    <div class="tl-item ${v.status}">
      ${statusHtml(v.status)}
      ${v.version_no ? `<b>v${v.version_no}</b> ·` : ""}
      <span class="mono">${esc(v.version_id)}</span><br>
      <span class="muted">守卫 ${esc(v.guard.slice(0, 12))}… ·
      基于快照 ${esc(v.based_on_run.slice(0, 12))}…</span>
    </div>`).join("") || '<span class="muted">暂无版本</span>';
}

async function loadJobs() {
  if (!state.datasetId) return;
  const { jobs } = await api(`/api/datasets/${state.datasetId}/jobs`);
  $("#jobList").innerHTML = jobs.map((j) => `
    <div class="meta-item">
      ${statusHtml(j.status)} <b>${j.job_type}</b>
      <span class="mono">${esc(j.job_id.slice(0, 14))}…</span><br>
      <span class="t">检查点 ${esc(j.checkpoint)} ${j.error ? "· 错误：" + esc(j.error) : ""}</span>
    </div>`).join("") || '<span class="muted">暂无任务</span>';
}

// ---------------------------------------------------------------------------
// ⑥ 查询与对比
// ---------------------------------------------------------------------------

$("#btnQuery").addEventListener("click", async () => {
  const version_id = $("#queryVersion").value;
  const path = $("#queryPath").value;
  if (!version_id || !path) return toast("请选择版本与路径", true);
  const op = $("#queryOp").value;
  const rawVal = $("#queryValue").value;
  const params = new URLSearchParams({ version: version_id, path, op });
  if (rawVal !== "") params.set("value", rawVal);
  const out = await api(`/api/datasets/${ds()}/query?${params}`);
  let html = `<div class="kv">主列扫描 <b>${out.main_scanned}</b> 行 · 影子列 <b>${out.shadow_scanned}</b> 行（影子不混入聚合）</div>`;
  if (out.aggregate !== null && out.aggregate !== undefined) {
    html += `<div class="kv">${op.toUpperCase()} 结果（仅主列强类型值）：<b>${esc(pretty(out.aggregate))}</b></div>`;
  }
  if (out.rows && out.rows.length) {
    html += `<table class="data-table"><thead><tr><th>row_id</th><th>实际类型</th><th>值</th><th>转换值</th></tr></thead><tbody>`
      + out.rows.map((r) => `<tr><td class="mono">${esc(r.row_id.slice(0, 12))}…</td>
        <td><span class="tag tag-${r.type}">${r.type}</span></td>
        <td class="mono">${esc(pretty(r.value))}</td>
        <td class="mono">${r.coerced === null ? "—" : esc(pretty(r.coerced))}</td></tr>`).join("")
      + "</tbody></table>";
  }
  if (out.shadow_rows && out.shadow_rows.length) {
    html += `<h3>影子列（独立成组，零丢失保留）</h3>`
      + out.shadow_rows.map((r) => `<div class="conv-card code-shadow">
        <span class="mono">${esc(r.row_id.slice(0, 12))}…</span>
        <span class="tag tag-${r.type}">${r.type}</span>
        <pre class="json">${esc(pretty(r.value))}</pre></div>`).join("");
  }
  $("#queryResult").innerHTML = html;
});

async function loadComparePaths() {
  if (!state.datasetId) return;
  const { latest } = await api(`/api/datasets/${state.datasetId}/inference`);
  if (!latest) { $("#comparePath").innerHTML = ""; return; }
  const conflictPaths = Object.values(latest.report.paths)
    .filter((p) => p.non_null_tags.length > 1 || p.advice.conflict_ratio > 0)
    .map((p) => p.path);
  $("#comparePath").innerHTML = conflictPaths
    .map((p) => `<option value="${esc(p)}">${esc(p)}</option>`).join("");
}

$("#btnCompare").addEventListener("click", async () => {
  const path = $("#comparePath").value;
  if (!path) return;
  const out = await api(`/api/datasets/${ds()}/compare?path=${encodeURIComponent(path)}`);
  const s = out.strategies;
  const card = (name, label, cls, detail) => `
    <div class="conv-card code-${cls}">
      <span class="chip ${name.toLowerCase()}">${name}</span> <b>${label}</b>
      ${detail}
      <div style="margin-top:8px">
        信息保留率 <b>${pct(s[name].retention)}</b>
        <div class="bar ${s[name].reject_ratio > 0 ? "danger" : ""}"><span style="width:${pct(s[name].retention)}"></span></div>
        主列强类型覆盖（查询能力）<b>${pct(s[name].query_power)}</b>
        <div class="bar"><span style="width:${pct(s[name].query_power)}"></span></div>
        <span class="muted">拒收 ${s[name].rejected || 0} 行 · 影子 ${s[name].shadowed || 0} · 联合变体 ${s[name].variants || 0}</span>
      </div>
    </div>`;
  $("#compareResult").innerHTML = `
    <div class="kv">路径 <span class="path">${esc(out.path)}</span> ·
      非空样本 ${out.total_non_null} · 承诺 ${tagsHtml(out.promised_tags)}</div>
    ${card("REJECT", "查询能力最强，牺牲拒收行的可参与性", "reject",
      '<span class="muted">下游强类型、可优化；所有拒收行进隔离区可回溯</span>')}
    ${card("UNION", "信息零损失，聚合查询需按判别子分流", "union",
      '<span class="muted">适合多类型都是合法业务语义的场景</span>')}
    ${card("SHADOW", "主分析查询能力与异类保真的折中", "shadow",
      '<span class="muted">主列维持承诺类型，影子列零丢失承接异类</span>')}`;
});

// ---------------------------------------------------------------------------
// ⑦ 审计与隔离
// ---------------------------------------------------------------------------

async function loadQuarantine() {
  if (!state.datasetId) return;
  const data = await api(`/api/datasets/${state.datasetId}/quarantine`);
  $("#quarantineBox").innerHTML = data.count
    ? `<table class="data-table wide"><thead><tr>
        <th>row_id</th><th>路径</th><th>实际类型</th><th>承诺</th>
        <th>原因</th><th>原始值</th><th>published</th>
      </tr></thead><tbody>`
      + data.items.map((q) => `<tr>
        <td class="mono">${esc(q.row_id.slice(0, 12))}…</td>
        <td class="path">${esc(q.path)}</td>
        <td><span class="tag tag-${q.actual_type}">${q.actual_type}</span></td>
        <td>${tagsHtml(q.promised)}</td>
        <td class="muted" style="font-size:11.5px">${esc(q.reason)}</td>
        <td class="mono" style="max-width:200px">${esc(pretty(q.raw_value_parsed))}</td>
        <td><span class="status status-failed">${q.published}（永不自动发布）</span></td>
      </tr>`).join("") + "</tbody></table>"
    : '<span class="muted">隔离区为空</span>';
}

async function loadAudit() {
  if (!state.datasetId) return;
  const { events } = await api(`/api/datasets/${state.datasetId}/audit`);
  $("#auditBox").innerHTML = events.map((e) => `
    <div class="meta-item">
      <b>${esc(e.event)}</b> <span class="t">${new Date(e.created_at * 1000).toLocaleTimeString()}</span>
      <pre class="json">${esc(pretty(e.detail))}</pre>
    </div>`).join("") || '<span class="muted">暂无审计事件</span>';
}

// ---------------------------------------------------------------------------
// 启动
// ---------------------------------------------------------------------------

(async function init() {
  try {
    await loadDatasets();
    if (state.datasetId) await fullRefresh();
  } catch (e) {
    toast("初始化失败：" + e.message, true);
  }
})();
