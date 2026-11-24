/**
 * Emby Proxy 流量监控 Worker（下拉对齐修复）
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
      });
    }

    if (url.pathname === "/collect" && request.method === "POST") {
      return handleCollect(request, env);
    }

    if (url.pathname === "/api/stats" && request.method === "GET") {
      return handleStats(url, env);
    }

    if (url.pathname === "/api/nodes" && request.method === "GET") {
      return handleNodes(env);
    }

    if (url.pathname === "/" || url.pathname === "/index.html") {
      return new Response(HTML, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    return new Response("Not Found", { status: 404 });
  },
};

function privacyLabel(s) {
  if (!s) return s;
  const len = s.length;
  if (len <= 1) return "*";
  if (len === 2) return s[0] + "*";
  return s[0] + "*".repeat(len - 2) + s[len - 1];
}

function privacyHost(host) {
  if (!host) return host;
  host = host.split(":")[0].toLowerCase();
  return host.split(".").map(privacyLabel).join(".");
}

function privacyIP(ip) {
  if (!ip) return ip;
  if (ip.includes(".") && !ip.includes(":")) {
    const parts = ip.split(".");
    if (parts.length === 4) {
      return `${parts[0]}.${parts[1]}.*.*`;
    }
    return ip;
  }
  if (ip.includes(":")) {
    const parts = ip.split(":");
    if (parts.length <= 2) return ip;
    return parts[0] + ":****:" + parts[parts.length - 1];
  }
  return ip;
}

function json(data) {
  return new Response(JSON.stringify(data), {
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  });
}

function mergeMap(target, source, maskFn) {
  if (!source) return;
  for (const [k, v] of Object.entries(source)) {
    const key = maskFn ? maskFn(k) : k;
    target[key] = (target[key] || 0) + v;
  }
}

async function handleCollect(request, env) {
  const token = env.REPORT_TOKEN || "";
  if (token) {
    const auth = request.headers.get("Authorization") || "";
    if (auth !== `Bearer ${token}`) {
      return new Response(JSON.stringify({ ok: false, error: "unauthorized" }), {
        status: 401,
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  let payload;
  try {
    payload = await request.json();
  } catch {
    return new Response(JSON.stringify({ ok: false, error: "invalid json" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  if (!payload.batch_id || !payload.period_date) {
    return new Response(JSON.stringify({ ok: false, error: "missing batch_id or period_date" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  if (!env.REPORTS) {
    return new Response(JSON.stringify({ ok: false, error: "KV not bound. Please bind REPORTS KV namespace" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }

  const seenKey = `seen:${payload.batch_id}`;
  const already = await env.REPORTS.get(seenKey);
  if (already) {
    return new Response(JSON.stringify({ ok: true, batch_id: payload.batch_id, duplicate: true }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  const nodeKey = payload.node_key || "unknown";
  const nodeNote = payload.note || payload.node || nodeKey;
  const date = payload.period_date;
  const hour = payload.period_hour || "daily";
  const isDaily = hour === "daily";

  const aggKey = `agg:${nodeKey}:${date}`;
  let agg = {};
  const raw = await env.REPORTS.get(aggKey);
  if (raw) {
    try { agg = JSON.parse(raw); } catch {}
  }

  if (!agg.date) {
    agg = {
      date,
      node_key: nodeKey,
      note: nodeNote,
      requests: 0,
      up_bytes: 0,
      down_bytes: 0,
      blocked: 0,
      hours: {},
      hosts: {},
      ips: {},
      countries: {},
      provinces: {},
    };
  }

  agg.requests += payload.requests || 0;
  agg.up_bytes += payload.up_bytes || 0;
  agg.down_bytes += payload.down_bytes || 0;
  agg.blocked += payload.blocked || 0;
  agg.note = nodeNote;

  if (!isDaily) {
    const h = String(hour).padStart(2, "0");
    if (!agg.hours[h]) agg.hours[h] = { requests: 0, up: 0, down: 0, blocked: 0 };
    agg.hours[h].requests += payload.requests || 0;
    agg.hours[h].up += payload.up_bytes || 0;
    agg.hours[h].down += payload.down_bytes || 0;
    agg.hours[h].blocked += payload.blocked || 0;
  }

  mergeMap(agg.hosts, payload.hosts, privacyHost);
  mergeMap(agg.ips, payload.ips, privacyIP);
  mergeMap(agg.countries, payload.countries);
  mergeMap(agg.provinces, payload.provinces);

  function prune(map, n = 40) {
    const entries = Object.entries(map).sort((a, b) => b[1] - a[1]);
    if (entries.length <= n) return map;
    return Object.fromEntries(entries.slice(0, n));
  }
  agg.hosts = prune(agg.hosts, 30);
  agg.ips = prune(agg.ips, 40);
  agg.countries = prune(agg.countries, 20);
  agg.provinces = prune(agg.provinces, 30);

  await env.REPORTS.put(aggKey, JSON.stringify(agg), {
    expirationTtl: 60 * 60 * 24 * 90,
  });

  await env.REPORTS.put(seenKey, "1", {
    expirationTtl: 60 * 60 * 24 * 7,
  });

  await updateNodeIndex(env, nodeKey, nodeNote);
  await updateReadyCache(env, "today");
  await updateReadyCache(env, "yesterday");
  await updateReadyCache(env, "7d");

  return new Response(JSON.stringify({ ok: true, batch_id: payload.batch_id }), {
    headers: { "Content-Type": "application/json" },
  });
}

async function updateNodeIndex(env, nodeKey, note) {
  const raw = await env.REPORTS.get("nodes:index");
  let nodes = {};
  if (raw) {
    try { nodes = JSON.parse(raw); } catch {}
  }
  nodes[nodeKey] = { note, last_seen: new Date().toISOString() };
  await env.REPORTS.put("nodes:index", JSON.stringify(nodes));
}

async function updateReadyCache(env, range) {
  try {
    const data = await computeStats(env, range, "");
    const key = `ready:${range}:all`;
    await env.REPORTS.put(key, JSON.stringify(data), {
      expirationTtl: 60 * 60 * 24 * 2,
    });
  } catch (e) {}
}

async function handleNodes(env) {
  if (!env.REPORTS) return json({ nodes: {} });
  const raw = await env.REPORTS.get("nodes:index");
  let nodes = {};
  if (raw) {
    try { nodes = JSON.parse(raw); } catch {}
  }
  return json({ nodes });
}

async function handleStats(url, env) {
  if (!env.REPORTS) {
    return json({ error: "KV not bound", summary: {}, series: [], top_hosts: [], top_ips: [], top_countries: [], top_provinces: [] });
  }

  const range = url.searchParams.get("range") || "today";
  const node = url.searchParams.get("node") || "";
  const customStart = url.searchParams.get("start") || "";
  const customEnd = url.searchParams.get("end") || "";

  if (!customStart && !node && ["today", "yesterday", "7d"].includes(range)) {
    const cached = await env.REPORTS.get(`ready:${range}:all`);
    if (cached) {
      try {
        return json(JSON.parse(cached));
      } catch {}
    }
  }

  const data = await computeStats(env, range, node, customStart, customEnd);
  return json(data);
}

async function computeStats(env, range, node, customStart = "", customEnd = "") {
  const now = new Date();
  const bj = new Date(now.getTime() + 8 * 3600 * 1000);
  const formatDate = (d) => d.toISOString().slice(0, 10);

  let startDate, endDate;
  if (customStart && customEnd) {
    startDate = customStart;
    endDate = customEnd;
  } else if (range === "today") {
    startDate = endDate = formatDate(bj);
  } else if (range === "yesterday") {
    const y = new Date(bj); y.setDate(y.getDate() - 1);
    startDate = endDate = formatDate(y);
  } else if (range === "7d") {
    const s = new Date(bj); s.setDate(s.getDate() - 6);
    startDate = formatDate(s);
    endDate = formatDate(bj);
  } else if (range === "30d") {
    const s = new Date(bj); s.setDate(s.getDate() - 29);
    startDate = formatDate(s);
    endDate = formatDate(bj);
  } else {
    startDate = "2000-01-01";
    endDate = formatDate(bj);
  }

  const dates = [];
  let cur = new Date(startDate + "T00:00:00Z");
  const end = new Date(endDate + "T00:00:00Z");
  while (cur <= end) {
    dates.push(cur.toISOString().slice(0, 10));
    cur.setDate(cur.getDate() + 1);
  }

  let nodeKeys = [];
  if (node) {
    nodeKeys = [node];
  } else {
    const raw = await env.REPORTS.get("nodes:index");
    if (raw) {
      try {
        const nodes = JSON.parse(raw);
        nodeKeys = Object.keys(nodes);
      } catch {}
    }
  }

  const aggs = [];
  for (const nk of nodeKeys) {
    for (const d of dates) {
      const raw = await env.REPORTS.get(`agg:${nk}:${d}`);
      if (raw) {
        try { aggs.push(JSON.parse(raw)); } catch {}
      }
    }
  }

  let totalRequests = 0, totalUp = 0, totalDown = 0, totalBlocked = 0;
  const ipSet = new Set();
  const hourly = {};
  const hosts = {};
  const countries = {};
  const provinces = {};
  const topIPs = {};

  for (const a of aggs) {
    totalRequests += a.requests || 0;
    totalUp += a.up_bytes || 0;
    totalDown += a.down_bytes || 0;
    totalBlocked += a.blocked || 0;

    if (a.hours) {
      for (const [h, v] of Object.entries(a.hours)) {
        const key = `${a.date} ${h}`;
        if (!hourly[key]) hourly[key] = { requests: 0, up: 0, down: 0, blocked: 0 };
        hourly[key].requests += v.requests || 0;
        hourly[key].up += v.up || 0;
        hourly[key].down += v.down || 0;
        hourly[key].blocked += v.blocked || 0;
      }
    }

    mergeMap(hosts, a.hosts);
    mergeMap(topIPs, a.ips);
    mergeMap(countries, a.countries);
    mergeMap(provinces, a.provinces);

    if (a.ips) {
      for (const ip of Object.keys(a.ips)) ipSet.add(ip);
    }
  }

  const series = [];
  if (dates.length === 1) {
    const d = dates[0];
    for (let h = 0; h < 24; h++) {
      const hh = String(h).padStart(2, "0");
      const key = `${d} ${hh}`;
      const v = hourly[key] || { requests: 0, up: 0, down: 0, blocked: 0 };
      series.push({
        label: `${hh}:00`,
        requests: v.requests,
        up: v.up,
        down: v.down,
        traffic: v.up + v.down,
        blocked: v.blocked,
      });
    }
  } else {
    Object.entries(hourly)
      .sort(([a], [b]) => a.localeCompare(b))
      .forEach(([k, v]) => {
        series.push({
          label: k.slice(5).replace(" ", " "),
          requests: v.requests,
          up: v.up,
          down: v.down,
          traffic: v.up + v.down,
          blocked: v.blocked,
        });
      });
  }

  const topHosts = Object.entries(hosts).sort((a, b) => b[1] - a[1]).slice(0, 50);
  const topIPList = Object.entries(topIPs).sort((a, b) => b[1] - a[1]).slice(0, 50);
  const topCountries = Object.entries(countries).sort((a, b) => b[1] - a[1]).slice(0, 30);
  const topProvinces = Object.entries(provinces).sort((a, b) => b[1] - a[1]).slice(0, 40);

  return {
    range: customStart ? "custom" : range,
    node: node || "all",
    startDate,
    endDate,
    summary: {
      requests: totalRequests,
      up_bytes: totalUp,
      down_bytes: totalDown,
      traffic_bytes: totalUp + totalDown,
      unique_ips: ipSet.size,
      blocked: totalBlocked,
    },
    series,
    top_hosts: topHosts,
    top_ips: topIPList,
    top_countries: topCountries,
    top_provinces: topProvinces,
    report_count: aggs.length,
  };
}

const HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>流量监控</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='22' fill='%233b82f6'/><path d='M18 72 L34 44 L50 58 L66 26 L82 50' stroke='white' stroke-width='9' fill='none' stroke-linecap='round' stroke-linejoin='round'/></svg>">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg: #f4f7fb;
  --card: #ffffff;
  --bd: #e6edf5;
  --t: #0f172a;
  --t2: #475569;
  --t3: #94a3b8;
  --p: #3b82f6;
  --shadow: 0 2px 8px -2px rgba(15, 23, 42, 0.06);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  background: #f4f7fb;
  color: var(--t);
  min-height: 100vh;
}
.header {
  background: rgba(255,255,255,0.95);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid var(--bd);
  padding: 8px 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 20;
}
.header-left { display: flex; align-items: center; gap: 8px; }
.header-left .icon {
  width: 28px; height: 28px; border-radius: 7px;
  background: linear-gradient(135deg, #3b82f6, #60a5fa);
  display: flex; align-items: center; justify-content: center; flex-shrink: 0;
}
.header-left h1 { font-size: 15px; font-weight: 700; }
.header-left .sub { font-size: 10px; color: var(--t3); }
.node-dropdown { position: relative; }
.node-btn {
  padding: 6px 28px 6px 12px;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  font-size: 12.5px;
  background: #fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%2364748b' stroke-width='2.5'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E") no-repeat right 8px center;
  background-size: 12px;
  min-width: 130px;
  color: var(--t);
  box-shadow: 0 1px 2px rgba(0,0,0,0.04);
  cursor: pointer;
  text-align: left;
}
.node-btn:hover { border-color: #cbd5e1; }
.node-menu {
  display: none;
  position: absolute;
  top: calc(100% + 4px);
  left: 0;
  background: #fff;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  box-shadow: 0 4px 16px -2px rgba(15,23,42,0.12);
  min-width: 100%;
  z-index: 30;
  overflow: hidden;
}
.node-menu.show { display: block; }
.node-item {
  padding: 7px 12px;
  font-size: 12.5px;
  color: var(--t);
  cursor: pointer;
  white-space: nowrap;
}
.node-item:hover { background: #f1f5f9; }
.node-item.active { background: #3b82f6; color: #fff; }
.container { padding: 10px 12px 24px; width: 100%; }
.filter-bar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
.tabs {
  display: flex; gap: 2px; background: #fff; padding: 3px;
  border-radius: 8px; border: 1px solid var(--bd); box-shadow: var(--shadow);
  overflow-x: auto; scrollbar-width: none;
}
.tabs::-webkit-scrollbar { display: none; }
.tab {
  padding: 4px 12px; border-radius: 6px; font-size: 12px;
  cursor: pointer; color: var(--t2); border: none; background: transparent;
  white-space: nowrap; flex-shrink: 0;
}
.tab.active { background: #3b82f6; color: #fff; font-weight: 600; }
.date-filter {
  display: none; align-items: center; gap: 4px;
  background: #fff; padding: 3px 6px; border-radius: 8px;
  border: 1px solid var(--bd); box-shadow: var(--shadow);
}
.date-filter.show { display: flex; }
.date-filter input[type="date"] {
  border: 1px solid #e2e8f0; border-radius: 5px; padding: 3px 6px; font-size: 12px;
}
.date-filter button {
  background: #3b82f6; color: #fff; border: none; border-radius: 5px;
  padding: 4px 10px; font-size: 12px; cursor: pointer;
}
.cards {
  display: grid; grid-template-columns: repeat(4, 1fr);
  gap: 10px; margin-bottom: 10px;
}
@media (max-width: 900px) { .cards { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 520px) { .cards { grid-template-columns: 1fr 1fr; } }
.card {
  background: var(--card); border: 1px solid var(--bd); border-radius: 10px;
  padding: 10px 14px; position: relative; box-shadow: var(--shadow);
}
.card::before {
  content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 3px; border-radius: 10px 0 0 10px;
}
.card.req::before { background: #3b82f6; }
.card.traffic::before { background: #10b981; }
.card.ip::before { background: #f59e0b; }
.card.block::before { background: #ef4444; }
.card .label { font-size: 11px; color: var(--t3); margin-bottom: 2px; }
.card .value { font-size: 20px; font-weight: 700; }
.card .sub { font-size: 10.5px; color: var(--t2); margin-top: 2px; }
.chart-card {
  background: var(--card); border: 1px solid var(--bd); border-radius: 10px;
  padding: 10px 14px 6px; margin-bottom: 10px; box-shadow: var(--shadow);
}
.chart-card h3 { font-size: 13px; font-weight: 650; margin-bottom: 1px; }
.chart-card .desc { font-size: 11px; color: var(--t3); margin-bottom: 6px; }
.chart-wrap { height: 150px; position: relative; }
.tables {
  display: grid; grid-template-columns: repeat(4, 1fr);
  gap: 10px; align-items: stretch;
}
@media (max-width: 1100px) { .tables { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 680px) { .tables { grid-template-columns: 1fr; } }
.table-card {
  background: var(--card); border: 1px solid var(--bd); border-radius: 10px;
  padding: 8px 12px 6px; box-shadow: var(--shadow);
  display: flex; flex-direction: column; height: 100%;
}
.table-card h3 { font-size: 12.5px; font-weight: 650; margin-bottom: 4px; flex-shrink: 0; }
.table-card table { width: 100%; border-collapse: collapse; font-size: 12px; }
th, td { text-align: left; padding: 3px 2px; border-bottom: 1px solid #f1f5f9; }
th { color: var(--t3); font-weight: 500; font-size: 11px; }
tr:last-child td { border-bottom: none; }
.pager {
  display: flex; align-items: center; justify-content: center;
  gap: 6px; margin-top: auto; padding-top: 6px;
  font-size: 11px; color: var(--t3); white-space: nowrap;
  border-top: 1px solid #f1f5f9; flex-shrink: 0;
}
.pager button {
  padding: 2px 6px; border: 1px solid var(--bd); background: #fff;
  border-radius: 4px; cursor: pointer; font-size: 11px; color: var(--t2); min-width: 46px;
}
.pager button:disabled { opacity: 0.35; cursor: not-allowed; }
.pager .page-info { min-width: 40px; text-align: center; }
.loading { text-align: center; padding: 40px 20px; color: var(--t3); font-size: 13px; }
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <div class="icon">
      <svg width="16" height="16" viewBox="0 0 100 100" fill="none">
        <path d="M16 74 L34 42 L50 58 L66 26 L84 50" stroke="white" stroke-width="12" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    </div>
    <div>
      <h1>流量监控</h1>
      <div class="sub">Emby Proxy 数据看板</div>
    </div>
  </div>
  <div class="node-dropdown">
    <button class="node-btn" id="nodeBtn">全部节点</button>
    <div class="node-menu" id="nodeMenu">
      <div class="node-item active" data-value="">全部节点</div>
    </div>
  </div>
</div>
<div class="container">
  <div class="filter-bar">
    <div class="tabs" id="tabs">
      <button class="tab active" data-range="today">今天</button>
      <button class="tab" data-range="yesterday">昨天</button>
      <button class="tab" data-range="7d">近 7 天</button>
      <button class="tab" data-range="30d">近 30 天</button>
      <button class="tab" data-range="all">全部</button>
      <button class="tab" data-range="custom">自定义</button>
    </div>
    <div class="date-filter" id="dateFilter">
      <input type="date" id="startDate">
      <span style="color:#94a3b8">至</span>
      <input type="date" id="endDate">
      <button onclick="loadCustom()">查询</button>
    </div>
  </div>
  <div id="loading" class="loading">加载中...</div>
  <div id="content" style="display:none">
    <div class="cards">
      <div class="card req"><div class="label">区间请求</div><div class="value" id="vRequests">0</div><div class="sub">当前筛选</div></div>
      <div class="card traffic"><div class="label">区间流量</div><div class="value" id="vTraffic">0 GB</div><div class="sub" id="vTrafficSub">上行 + 下行</div></div>
      <div class="card ip"><div class="label">独立 IP</div><div class="value" id="vIPs">0</div><div class="sub">当前筛选</div></div>
      <div class="card block"><div class="label">拦截次数</div><div class="value" id="vBlocked">0</div><div class="sub">当前筛选</div></div>
    </div>
    <div class="chart-card">
      <h3>流量消耗趋势（分时）</h3>
      <div class="desc">各时段网卡流量（下行=接收，上行=发送）</div>
      <div class="chart-wrap"><canvas id="trafficChart"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>请求量趋势（分时）</h3>
      <div class="desc">各时段请求数（北京时间）</div>
      <div class="chart-wrap"><canvas id="reqChart"></canvas></div>
    </div>
    <div class="tables">
      <div class="table-card" data-key="hosts">
        <h3>Top 主机</h3>
        <table><thead><tr><th>主机</th><th>请求</th></tr></thead><tbody id="tbHosts"></tbody></table>
        <div class="pager" id="pagerHosts"></div>
      </div>
      <div class="table-card" data-key="ips">
        <h3>Top IP</h3>
        <table><thead><tr><th>IP</th><th>请求</th></tr></thead><tbody id="tbIPs"></tbody></table>
        <div class="pager" id="pagerIPs"></div>
      </div>
      <div class="table-card" data-key="countries">
        <h3>国家/地区</h3>
        <table><thead><tr><th>地区</th><th>请求</th></tr></thead><tbody id="tbCountries"></tbody></table>
        <div class="pager" id="pagerCountries"></div>
      </div>
      <div class="table-card" data-key="provinces">
        <h3>省份排行</h3>
        <table><thead><tr><th>省份</th><th>请求</th></tr></thead><tbody id="tbProvinces"></tbody></table>
        <div class="pager" id="pagerProvinces"></div>
      </div>
    </div>
  </div>
</div>
<script>
let trafficChart, reqChart;
let currentRange = "today";
let currentNode = "";
let customStart = "";
let customEnd = "";
const PAGE_SIZE = 8;
const tableState = {
  hosts: { data: [], page: 0 },
  ips: { data: [], page: 0 },
  countries: { data: [], page: 0 },
  provinces: { data: [], page: 0 },
};

const today = new Date().toISOString().slice(0, 10);
document.getElementById("startDate").value = today;
document.getElementById("endDate").value = today;

const nodeBtn = document.getElementById("nodeBtn");
const nodeMenu = document.getElementById("nodeMenu");
nodeBtn.addEventListener("click", (e) => {
  e.stopPropagation();
  nodeMenu.classList.toggle("show");
});
document.addEventListener("click", () => nodeMenu.classList.remove("show"));
nodeMenu.addEventListener("click", (e) => {
  const item = e.target.closest(".node-item");
  if (!item) return;
  currentNode = item.dataset.value || "";
  nodeBtn.textContent = item.textContent;
  nodeMenu.querySelectorAll(".node-item").forEach(el => el.classList.remove("active"));
  item.classList.add("active");
  nodeMenu.classList.remove("show");
  loadData();
});

document.getElementById("tabs").addEventListener("click", e => {
  if (e.target.classList.contains("tab")) {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    e.target.classList.add("active");
    currentRange = e.target.dataset.range;
    const dateFilter = document.getElementById("dateFilter");
    if (currentRange === "custom") {
      dateFilter.classList.add("show");
    } else {
      dateFilter.classList.remove("show");
      customStart = "";
      customEnd = "";
      loadData();
    }
  }
});

function loadCustom() {
  const s = document.getElementById("startDate").value;
  const e = document.getElementById("endDate").value;
  if (!s || !e) return alert("请选择开始和结束日期");
  if (s > e) return alert("开始日期不能晚于结束日期");
  customStart = s;
  customEnd = e;
  loadData();
}

function formatBytes(b) {
  if (b >= 1e12) return (b / 1e12).toFixed(2) + " TB";
  if (b >= 1e9) return (b / 1e9).toFixed(2) + " GB";
  if (b >= 1e6) return (b / 1e6).toFixed(2) + " MB";
  return (b / 1e3).toFixed(1) + " KB";
}

function renderTable(key, tbodyId, pagerId) {
  const state = tableState[key];
  const data = state.data || [];
  const page = state.page;
  const start = page * PAGE_SIZE;
  const slice = data.slice(start, start + PAGE_SIZE);
  const tb = document.getElementById(tbodyId);
  tb.innerHTML = slice.length
    ? slice.map(([k, v]) => \`<tr><td>\${k}</td><td>\${v.toLocaleString()}</td></tr>\`).join("")
    : '<tr><td colspan="2" style="color:#94a3b8">暂无数据</td></tr>';

  const totalPages = Math.max(1, Math.ceil(data.length / PAGE_SIZE));
  const pager = document.getElementById(pagerId);
  pager.innerHTML = \`
    <button \${page <= 0 ? "disabled" : ""} onclick="changePage('\${key}', -1)">上一页</button>
    <span class="page-info">\${page + 1} / \${totalPages}</span>
    <button \${page >= totalPages - 1 ? "disabled" : ""} onclick="changePage('\${key}', 1)">下一页</button>
  \`;
}

function changePage(key, delta) {
  const state = tableState[key];
  const totalPages = Math.max(1, Math.ceil(state.data.length / PAGE_SIZE));
  state.page = Math.max(0, Math.min(totalPages - 1, state.page + delta));
  const map = { hosts: ["tbHosts", "pagerHosts"], ips: ["tbIPs", "pagerIPs"], countries: ["tbCountries", "pagerCountries"], provinces: ["tbProvinces", "pagerProvinces"] };
  renderTable(key, map[key][0], map[key][1]);
}
window.changePage = changePage;

async function loadNodes() {
  try {
    const r = await fetch("/api/nodes");
    const data = await r.json();
    const menu = document.getElementById("nodeMenu");
    Object.entries(data.nodes || {}).forEach(([k, v]) => {
      const div = document.createElement("div");
      div.className = "node-item";
      div.dataset.value = k;
      div.textContent = v.note || k;
      menu.appendChild(div);
    });
  } catch (e) {}
}

async function loadData() {
  document.getElementById("loading").style.display = "block";
  document.getElementById("content").style.display = "none";
  try {
    const qs = new URLSearchParams({ range: currentRange, node: currentNode });
    if (customStart && customEnd) {
      qs.set("start", customStart);
      qs.set("end", customEnd);
    }
    const r = await fetch("/api/stats?" + qs);
    const data = await r.json();
    render(data);
  } catch (e) {
    document.getElementById("loading").textContent = "加载失败: " + e.message;
  }
}

function render(data) {
  const s = data.summary || {};
  document.getElementById("vRequests").textContent = (s.requests || 0).toLocaleString();
  document.getElementById("vTraffic").textContent = formatBytes(s.traffic_bytes || 0);
  document.getElementById("vTrafficSub").textContent = \`上行 \${formatBytes(s.up_bytes || 0)} · 下行 \${formatBytes(s.down_bytes || 0)}\`;
  document.getElementById("vIPs").textContent = (s.unique_ips || 0).toLocaleString();
  document.getElementById("vBlocked").textContent = (s.blocked || 0).toLocaleString();

  const labels = (data.series || []).map(x => x.label);
  const trafficData = (data.series || []).map(x => ((x.traffic || 0) / 1e9).toFixed(2));
  const reqData = (data.series || []).map(x => x.requests);

  if (trafficChart) trafficChart.destroy();
  trafficChart = new Chart(document.getElementById("trafficChart"), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "流量 (GB)",
        data: trafficData,
        borderColor: "#10b981",
        backgroundColor: "rgba(16,185,129,0.12)",
        fill: true,
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#0f172a",
          padding: 8,
          cornerRadius: 6,
          callbacks: { label: ctx => "流量: " + ctx.parsed.y + " GB" }
        }
      },
      scales: {
        y: { beginAtZero: true, grid: { color: "#f1f5f9" }, ticks: { color: "#94a3b8", font: { size: 10 } } },
        x: { grid: { display: false }, ticks: { color: "#94a3b8", font: { size: 10 }, maxRotation: 0 } }
      }
    }
  });

  if (reqChart) reqChart.destroy();
  reqChart = new Chart(document.getElementById("reqChart"), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "请求数",
        data: reqData,
        borderColor: "#3b82f6",
        backgroundColor: "rgba(59,130,246,0.12)",
        fill: true,
        tension: 0.4,
        pointRadius: 0,
        pointHoverRadius: 4,
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#0f172a",
          padding: 8,
          cornerRadius: 6,
          callbacks: { label: ctx => "请求: " + ctx.parsed.y.toLocaleString() }
        }
      },
      scales: {
        y: { beginAtZero: true, grid: { color: "#f1f5f9" }, ticks: { color: "#94a3b8", font: { size: 10 } } },
        x: { grid: { display: false }, ticks: { color: "#94a3b8", font: { size: 10 }, maxRotation: 0 } }
      }
    }
  });

  tableState.hosts = { data: data.top_hosts || [], page: 0 };
  tableState.ips = { data: data.top_ips || [], page: 0 };
  tableState.countries = { data: data.top_countries || [], page: 0 };
  tableState.provinces = { data: data.top_provinces || [], page: 0 };

  renderTable("hosts", "tbHosts", "pagerHosts");
  renderTable("ips", "tbIPs", "pagerIPs");
  renderTable("countries", "tbCountries", "pagerCountries");
  renderTable("provinces", "tbProvinces", "pagerProvinces");

  document.getElementById("loading").style.display = "none";
  document.getElementById("content").style.display = "block";
}

loadNodes();
loadData();
</script>
</body>
</html>`;
