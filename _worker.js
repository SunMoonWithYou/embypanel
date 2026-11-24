/**
 * Emby Proxy 流量监控 Worker（完整优化版）
 * 特性：旁路缓存、防并发覆盖、低 KV 消耗、并发拉取、前端四列分色
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

// ================= 工具函数 =================

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
    if (parts.length === 4) return `${parts[0]}.${parts[1]}.*.*`;
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

// ================= 上报收集逻辑 =================

async function handleCollect(request, env) {
  const token = env.REPORT_TOKEN || "";
  if (token) {
    const auth = request.headers.get("Authorization") || "";
    if (auth !== `Bearer ${token}`) {
      return new Response(JSON.stringify({ ok: false, error: "unauthorized" }), {
        status: 401, headers: { "Content-Type": "application/json" },
      });
    }
  }

  let payload;
  try {
    payload = await request.json();
  } catch {
    return new Response(JSON.stringify({ ok: false, error: "invalid json" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }

  if (!payload.batch_id || !payload.period_date) {
    return new Response(JSON.stringify({ ok: false, error: "missing batch_id or period_date" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }

  if (!env.REPORTS) {
    return new Response(JSON.stringify({ ok: false, error: "KV not bound" }), {
      status: 500, headers: { "Content-Type": "application/json" },
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
      date, node_key: nodeKey, note: nodeNote,
      requests: 0, up_bytes: 0, down_bytes: 0, blocked: 0,
      hours: {}, hosts: {}, ips: {}, countries: {}, provinces: {}, clients: {},
    };
  }
  if (!agg.clients) agg.clients = {};

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
  mergeMap(agg.clients, payload.clients);

  const prune = (map, n) => {
    const entries = Object.entries(map).sort((a, b) => b[1] - a[1]);
    return Object.fromEntries(entries.slice(0, n));
  };
  agg.hosts = prune(agg.hosts, 30);
  agg.ips = prune(agg.ips, 40);
  agg.countries = prune(agg.countries, 20);
  agg.provinces = prune(agg.provinces, 30);
  agg.clients = prune(agg.clients, 30);

  // 双写并发
  await Promise.all([
    env.REPORTS.put(aggKey, JSON.stringify(agg), { expirationTtl: 60 * 60 * 24 * 90 }),
    env.REPORTS.put(seenKey, "1", { expirationTtl: 60 * 60 * 24 * 7 })
  ]);

  // 异步静默更新节点索引
  updateNodeIndex(env, nodeKey, nodeNote).catch(() => {});

  return new Response(JSON.stringify({ ok: true, batch_id: payload.batch_id }), {
    headers: { "Content-Type": "application/json" },
  });
}

// ================= 节点索引防冲突 =================

async function updateNodeIndex(env, nodeKey, note) {
  try {
    const raw = await env.REPORTS.get("nodes:index");
    let nodes = raw ? JSON.parse(raw) : {};
    
    // 节流：1小时内相同节点不重复写入，防止并发冲突
    const now = Date.now();
    if (nodes[nodeKey] && nodes[nodeKey].note === note) {
      const lastSeen = new Date(nodes[nodeKey].last_seen).getTime();
      if (now - lastSeen < 60 * 60 * 1000) return; 
    }

    nodes[nodeKey] = { note, last_seen: new Date().toISOString() };
    await env.REPORTS.put("nodes:index", JSON.stringify(nodes));
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

// ================= 数据统计与缓存旁路 =================

async function handleStats(url, env) {
  if (!env.REPORTS) {
    return json({ error: "KV not bound", summary: {}, series: [], top_hosts: [], top_ips: [], top_countries: [], top_provinces: [], top_clients: [] });
  }

  const range = url.searchParams.get("range") || "today";
  const node = url.searchParams.get("node") || "";
  const customStart = url.searchParams.get("start") || "";
  const customEnd = url.searchParams.get("end") || "";

  // 如果是默认查询（今天/昨天/7天/30天）且未指定节点，优先走缓存
  if (!customStart && !node && ["today", "yesterday", "7d", "30d"].includes(range)) {
    const cacheKey = `ready:${range}:all`;
    const cached = await env.REPORTS.get(cacheKey);
    
    if (cached) {
      try { return json(JSON.parse(cached)); } catch {}
    }

    // 缓存未命中，现场计算
    const data = await computeStats(env, range, node, customStart, customEnd);
    
    // 一小时上报一次，缓存30分钟(1800秒)能够覆盖大部分访问，极大节省 KV 读
    env.REPORTS.put(cacheKey, JSON.stringify(data), { expirationTtl: 1800 }).catch(() => {});
    return json(data);
  }

  // 自定义时间或指定节点：直接实时计算
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
      try { nodeKeys = Object.keys(JSON.parse(raw)); } catch {}
    }
  }

  // 利用 Promise.all 并发拉取数据（分批次防超限）
  const aggs = [];
  const tasks = [];
  for (const nk of nodeKeys) {
    for (const d of dates) {
      tasks.push(env.REPORTS.get(`agg:${nk}:${d}`));
    }
  }
  
  // Cloudflare 免费版限制单次 Worker 最多 50 个 subrequest
  // 这里采用分块并发执行，每次并发 40 个，避免大跨度查询时超限
  const CHUNK_SIZE = 40;
  for (let i = 0; i < tasks.length; i += CHUNK_SIZE) {
    const chunk = tasks.slice(i, i + CHUNK_SIZE);
    const results = await Promise.all(chunk);
    for (const raw of results) {
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
  const clients = {};
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
    mergeMap(clients, a.clients);

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
  const topClients = Object.entries(clients).sort((a, b) => b[1] - a[1]).slice(0, 40);

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
    top_clients: topClients,
    report_count: aggs.length,
  };
}

// ================= 前端 HTML (带有主题配色) =================
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
  --bg: #f4f7fc;
  --card: #ffffff;
  --bd: #e6edf7;
  --bd-strong: #d8e4f5;
  --t: #0f172a;
  --t2: #475569;
  --t3: #94a3b8;
  --blue: #3b82f6;
  --green: #10b981;
  --amber: #f59e0b;
  --red: #ef4444;
  --purple: #8b5cf6;
  --radius: 14px;
  --shadow-sm: 0 1px 2px rgba(15,23,42,.05);
  --shadow: 0 4px 14px -8px rgba(15,23,42,.18), 0 1px 2px rgba(15,23,42,.04);
  --shadow-lg: 0 16px 34px -18px rgba(15,23,42,.32), 0 2px 6px rgba(15,23,42,.05);
}

* { box-sizing: border-box; margin: 0; padding: 0; }
html { -webkit-text-size-adjust: 100%; }

body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
  color: var(--t);
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  background:
    radial-gradient(920px 480px at 8% -10%, #e2edff 0%, rgba(226,237,255,0) 62%),
    radial-gradient(780px 420px at 100% -8%, #e9e7ff 0%, rgba(233,231,255,0) 58%),
    var(--bg);
  background-attachment: fixed;
}

::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #d5dfec; border-radius: 99px; }
::-webkit-scrollbar-thumb:hover { background: #bfcde0; }

/* ---------- 顶栏 ---------- */
.header {
  position: sticky;
  top: 0;
  z-index: 20;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 16px;
  background: rgba(255,255,255,.82);
  backdrop-filter: saturate(180%) blur(14px);
  -webkit-backdrop-filter: saturate(180%) blur(14px);
  border-bottom: 1px solid rgba(226,235,247,.9);
}

.header-left { display: flex; align-items: center; gap: 10px; }

.header-left .icon {
  width: 34px;
  height: 34px;
  border-radius: 10px;
  background: linear-gradient(135deg, #60a5fa, #3b82f6);
  display: grid;
  place-items: center;
  flex-shrink: 0;
  box-shadow: 0 6px 14px -8px rgba(59,130,246,.95);
}

.header-left h1 { font-size: 15.5px; font-weight: 700; letter-spacing: -.2px; line-height: 1.2; }
.header-left .sub { font-size: 10.5px; color: var(--t3); margin-top: 2px; letter-spacing: .2px; }

/* ---------- 节点下拉 ---------- */
.node-dropdown { position: relative; }

.node-btn {
  display: block;
  min-width: 122px;
  padding: 7px 30px 7px 12px;
  border: 1px solid var(--bd);
  border-radius: 10px;
  font-size: 12.5px;
  font-weight: 500;
  font-family: inherit;
  color: var(--t);
  background: #fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%2364748b' stroke-width='2.5' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E") no-repeat right 10px center;
  background-size: 11px;
  box-shadow: var(--shadow-sm);
  cursor: pointer;
  text-align: left;
  transition: border-color .16s, box-shadow .16s, color .16s;
}
.node-btn:hover { border-color: #c3dafa; color: var(--blue); box-shadow: 0 6px 14px -8px rgba(59,130,246,.7); }

.node-menu {
  display: none;
  position: absolute;
  top: calc(100% + 6px);
  right: 0;
  min-width: 100%;
  max-height: 300px;
  overflow: auto;
  padding: 4px;
  background: #fff;
  border: 1px solid var(--bd);
  border-radius: 12px;
  box-shadow: var(--shadow-lg);
  z-index: 30;
  animation: menuIn .16s ease both;
}
.node-menu.show { display: block; }
@keyframes menuIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: none; } }

.node-item {
  padding: 8px 12px;
  font-size: 12.5px;
  color: var(--t);
  border-radius: 8px;
  cursor: pointer;
  white-space: nowrap;
  transition: background .14s, color .14s;
}
.node-item:hover { background: #f1f6ff; color: var(--blue); }
.node-item.active { background: linear-gradient(135deg, #4f8ef7, #3b82f6); color: #fff; font-weight: 600; }

/* ---------- 容器 ---------- */
.container { width: 100%; max-width: 1580px; margin: 0 auto; padding: 14px 16px 34px; }

/* ---------- 筛选条 ---------- */
.filter-bar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 14px; }

.tabs {
  display: flex;
  gap: 3px;
  padding: 4px;
  background: #fff;
  border: 1px solid var(--bd);
  border-radius: 12px;
  box-shadow: var(--shadow-sm);
  overflow-x: auto;
  scrollbar-width: none;
}
.tabs::-webkit-scrollbar { display: none; }

.tab {
  padding: 6px 14px;
  border: none;
  border-radius: 8px;
  font-family: inherit;
  font-size: 12.5px;
  font-weight: 500;
  color: var(--t2);
  background: transparent;
  cursor: pointer;
  white-space: nowrap;
  flex-shrink: 0;
  transition: background .18s, color .18s, box-shadow .18s;
}
.tab:hover { background: #f1f5fb; color: var(--t); }
.tab.active {
  background: linear-gradient(135deg, #4f8ef7, #3b82f6);
  color: #fff;
  font-weight: 600;
  box-shadow: 0 5px 12px -6px rgba(59,130,246,.95);
}

.date-filter {
  display: none;
  align-items: center;
  gap: 6px;
  padding: 4px 8px;
  background: #fff;
  border: 1px solid var(--bd);
  border-radius: 12px;
  box-shadow: var(--shadow-sm);
}
.date-filter.show { display: flex; }

.date-filter input[type="date"] {
  padding: 5px 8px;
  font-family: inherit;
  font-size: 12px;
  color: var(--t);
  background: #fbfdff;
  border: 1px solid #e2e8f0;
  border-radius: 7px;
  outline: none;
  transition: border-color .16s, box-shadow .16s;
}
.date-filter input[type="date"]:focus { border-color: #93c5fd; box-shadow: 0 0 0 3px rgba(59,130,246,.12); }

.date-filter button {
  padding: 6px 14px;
  font-family: inherit;
  font-size: 12.5px;
  font-weight: 600;
  color: #fff;
  background: linear-gradient(135deg, #4f8ef7, #3b82f6);
  border: none;
  border-radius: 7px;
  cursor: pointer;
  box-shadow: 0 5px 12px -6px rgba(59,130,246,.95);
  transition: filter .16s, transform .16s;
}
.date-filter button:hover { filter: brightness(1.06); transform: translateY(-1px); }
.date-filter button:active { transform: translateY(0); }

/* ---------- 指标卡 ---------- */
.cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 12px; }
@media (max-width: 900px) { .cards { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 420px) { .cards { grid-template-columns: 1fr; } }

.card {
  position: relative;
  overflow: hidden;
  padding: 14px 16px 13px;
  background: var(--card);
  border: 1px solid var(--bd);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  transition: transform .2s cubic-bezier(.22,1,.36,1), box-shadow .2s, border-color .2s;
}
.card:hover { transform: translateY(-3px); box-shadow: var(--shadow-lg); border-color: var(--bd-strong); }

.card::before {
  content: "";
  position: absolute;
  top: -46px;
  right: -46px;
  width: 130px;
  height: 130px;
  border-radius: 50%;
  background: var(--accent);
  opacity: .10;
  pointer-events: none;
}

.card.req { --accent: #3b82f6; }
.card.traffic { --accent: #10b981; }
.card.ip { --accent: #f59e0b; }
.card.block { --accent: #ef4444; }

.card-top { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }

.card-icon {
  width: 26px;
  height: 26px;
  border-radius: 8px;
  display: grid;
  place-items: center;
  flex-shrink: 0;
  background: rgba(59,130,246,.10);
  color: #3b82f6;
}
.card.traffic .card-icon { background: rgba(16,185,129,.11); color: #10b981; }
.card.ip .card-icon { background: rgba(245,158,11,.12); color: #f59e0b; }
.card.block .card-icon { background: rgba(239,68,68,.11); color: #ef4444; }

.card .label {
  font-size: 11.5px;
  font-weight: 500;
  color: var(--t3);
  letter-spacing: .3px;
  margin-bottom: 0;
}

.card .value {
  font-size: 24px;
  font-weight: 700;
  letter-spacing: -.6px;
  line-height: 1.15;
  font-variant-numeric: tabular-nums;
}

.card .sub {
  margin-top: 5px;
  font-size: 11px;
  color: var(--t3);
  font-variant-numeric: tabular-nums;
}

/* ---------- 图表卡 ---------- */
.chart-card {
  padding: 14px 16px 8px;
  margin-bottom: 12px;
  background: var(--card);
  border: 1px solid var(--bd);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
}

.chart-card h3 {
  display: flex;
  align-items: center;
  gap: 7px;
  font-size: 13.5px;
  font-weight: 650;
  letter-spacing: -.2px;
}
.chart-card h3::before {
  content: "";
  width: 6px;
  height: 6px;
  border-radius: 2px;
  background: currentColor;
  flex-shrink: 0;
}
.chart-card:nth-of-type(1) h3 { color: #0f9f74; }
.chart-card:nth-of-type(2) h3 { color: #2563eb; }
.chart-card:nth-of-type(1) h3::before { background: #10b981; }
.chart-card:nth-of-type(2) h3::before { background: #3b82f6; }

.chart-card .desc { margin: 3px 0 10px; font-size: 11.5px; color: var(--t3); }
.chart-wrap { position: relative; height: 168px; }
@media (max-width: 680px) { .chart-wrap { height: 142px; } }

/* ---------- 排行榜 ---------- */
.tables { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; align-items: stretch; }
@media (max-width: 1240px) { .tables { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 700px) { .tables { grid-template-columns: 1fr; } }

.table-card {
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: 14px 14px 10px;
  background: var(--card);
  border: 1px solid var(--bd);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  transition: box-shadow .2s, border-color .2s;
}
.table-card:hover { box-shadow: var(--shadow-lg); border-color: var(--bd-strong); }

.table-card h3 {
  display: flex;
  align-items: center;
  gap: 7px;
  margin-bottom: 2px;
  font-size: 13.5px;
  font-weight: 650;
  letter-spacing: -.2px;
  flex-shrink: 0;
}
.table-card h3::before {
  content: "";
  width: 6px;
  height: 6px;
  border-radius: 2px;
  background: var(--blue);
  flex-shrink: 0;
}
.table-card[data-key="hosts"] h3::before { background: #3b82f6; }
.table-card[data-key="ips"] h3::before { background: #8b5cf6; }
.table-card[data-key="regions"] h3::before { background: #f59e0b; }
.table-card[data-key="clients"] h3::before { background: #10b981; }

.table-card .desc { margin-bottom: 9px; font-size: 11.5px; color: var(--t3); }

.bar-list { flex: 1; min-height: 0; }

.bar-item {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 7px;
  font-size: 12px;
}
.bar-item.sub { padding-left: 13px; }

.bar-label {
  width: 76px;
  flex-shrink: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--t2);
  font-size: 11.5px;
}
.bar-item.sub .bar-label { color: var(--t3); width: 68px; }

.bar-label.clickable { cursor: pointer; color: #2563eb; font-weight: 500; }
.bar-label.clickable:hover { text-decoration: underline; }

.bar-track {
  flex: 1;
  height: 7px;
  background: #f1f5f9;
  border-radius: 99px;
  overflow: hidden;
}

/* ---------- 进度条配色（按不同模块着色） ---------- */
.bar-fill {
  height: 100%;
  border-radius: 99px;
  transition: width .55s cubic-bezier(.22,1,.36,1);
}

/* 主机 - 蓝色 */
.table-card[data-key="hosts"] .bar-fill {
  background: linear-gradient(90deg, #60a5fa, #3b82f6);
}

/* IP - 紫色 */
.table-card[data-key="ips"] .bar-fill {
  background: linear-gradient(90deg, #a78bfa, #8b5cf6);
}

/* 地区 - 橙色 */
.table-card[data-key="regions"] .bar-fill {
  background: linear-gradient(90deg, #fbbf24, #f59e0b);
}
/* 地区省份子级 - 浅橙色 */
.table-card[data-key="regions"] .bar-fill.sub {
  background: linear-gradient(90deg, #fde68a, #fcd34d);
}

/* 播放器 - 绿色 */
.table-card[data-key="clients"] .bar-fill {
  background: linear-gradient(90deg, #34d399, #10b981);
}

.bar-nums {
  display: flex;
  align-items: baseline;
  justify-content: flex-end;
  gap: 5px;
  flex-shrink: 0;
  min-width: 74px;
}
.bar-num {
  font-size: 12px;
  font-weight: 600;
  color: var(--t);
  font-variant-numeric: tabular-nums;
}
.bar-pct {
  min-width: 26px;
  text-align: right;
  font-size: 10.5px;
  color: #a8b4c4;
  font-variant-numeric: tabular-nums;
}

/* ---------- 分页 ---------- */
.pager {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px dashed #eef2f8;
  font-size: 11.5px;
  color: var(--t3);
  white-space: nowrap;
  flex-shrink: 0;
}

.pager button {
  padding: 4px 11px;
  font-family: inherit;
  font-size: 11.5px;
  color: var(--t2);
  background: #fff;
  border: 1px solid var(--bd);
  border-radius: 7px;
  cursor: pointer;
  transition: background .15s, border-color .15s, color .15s;
}
.pager button:not(:disabled):hover { background: #f1f6ff; border-color: #c9ddfa; color: var(--blue); }
.pager button:disabled { opacity: .38; cursor: not-allowed; }
.pager .page-info { min-width: 52px; text-align: center; font-variant-numeric: tabular-nums; }

/* ---------- 状态 ---------- */
.loading {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 64px 20px;
  font-size: 13px;
  color: var(--t3);
}
.loading::before {
  content: "";
  width: 16px;
  height: 16px;
  border: 2px solid #dbeafe;
  border-top-color: #3b82f6;
  border-radius: 50%;
  animation: spin .7s linear infinite;
  flex-shrink: 0;
}
@keyframes spin { to { transform: rotate(360deg); } }

#content { animation: fadeUp .4s cubic-bezier(.22,1,.36,1) both; }

@keyframes fadeUp {
  from { opacity: 0; transform: translateY(8px); }
  to { opacity: 1; transform: none; }
}

.cards .card { animation: fadeUp .45s cubic-bezier(.22,1,.36,1) both; }
.cards .card:nth-child(1) { animation-delay: .02s; }
.cards .card:nth-child(2) { animation-delay: .06s; }
.cards .card:nth-child(3) { animation-delay: .10s; }
.cards .card:nth-child(4) { animation-delay: .14s; }

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation-duration: .001ms !important; transition-duration: .001ms !important; }
}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <div class="icon">
      <svg width="18" height="18" viewBox="0 0 100 100" fill="none">
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
      <span style="color:#94a3b8;font-size:12px">至</span>
      <input type="date" id="endDate">
      <button onclick="loadCustom()">查询</button>
    </div>
  </div>

  <div id="loading" class="loading">加载中...</div>

  <div id="content" style="display:none">
    <div class="cards">
      <div class="card req">
        <div class="card-top">
          <span class="card-icon">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M5 20V11"/><path d="M12 20V4"/><path d="M19 20v-6"/>
            </svg>
          </span>
          <span class="label">区间请求</span>
        </div>
        <div class="value" id="vRequests">0</div>
        <div class="sub">当前筛选</div>
      </div>

      <div class="card traffic">
        <div class="card-top">
          <span class="card-icon">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M7 20V5"/><path d="M3.5 8.5L7 5l3.5 3.5"/>
              <path d="M17 4v15"/><path d="M20.5 15.5L17 19l-3.5-3.5"/>
            </svg>
          </span>
          <span class="label">区间流量</span>
        </div>
        <div class="value" id="vTraffic">0 GB</div>
        <div class="sub" id="vTrafficSub">上行 + 下行</div>
      </div>

      <div class="card ip">
        <div class="card-top">
          <span class="card-icon">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="9"/><path d="M3 12h18"/><ellipse cx="12" cy="12" rx="4" ry="9"/>
            </svg>
          </span>
          <span class="label">独立 IP</span>
        </div>
        <div class="value" id="vIPs">0</div>
        <div class="sub">当前筛选</div>
      </div>

      <div class="card block">
        <div class="card-top">
          <span class="card-icon">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M12 3l7 3v6c0 4.4-3 7.6-7 9-4-1.4-7-4.6-7-9V6l7-3z"/>
              <path d="M9.5 9.5l5 5"/><path d="M14.5 9.5l-5 5"/>
            </svg>
          </span>
          <span class="label">拦截次数</span>
        </div>
        <div class="value" id="vBlocked">0</div>
        <div class="sub">当前筛选</div>
      </div>
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
        <div class="bar-list" id="tbHosts"></div>
        <div class="pager" id="pagerHosts"></div>
      </div>
      <div class="table-card" data-key="ips">
        <h3>Top IP</h3>
        <div class="bar-list" id="tbIPs"></div>
        <div class="pager" id="pagerIPs"></div>
      </div>
      <div class="table-card" data-key="regions">
        <h3>地区分布</h3>
        <div class="bar-list" id="tbRegions"></div>
      </div>
      <div class="table-card" data-key="clients">
        <h3>播放器分布</h3>
        <div class="desc">按客户端归类</div>
        <div class="bar-list" id="tbClients"></div>
        <div class="pager" id="pagerClients"></div>
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
  clients: { data: [], page: 0 },
};
let regionExpanded = false;
let regionCountries = [];
let regionProvinces = [];

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

function renderTable(key, listId, pagerId) {
  const state = tableState[key];
  const data = state.data || [];
  const page = state.page;
  const start = page * PAGE_SIZE;
  const slice = data.slice(start, start + PAGE_SIZE);
  const list = document.getElementById(listId);
  const max = data.length ? Math.max(...data.map(x => x[1])) : 1;
  const total = data.reduce((s, x) => s + x[1], 0) || 1;

  list.innerHTML = slice.length
    ? slice.map(([k, v]) => {
        const pct = Math.round(v / total * 100);
        const width = Math.max(2, (v / max) * 100);
        return \`<div class="bar-item">
          <div class="bar-label" title="\${k}">\${k}</div>
          <div class="bar-track"><div class="bar-fill" style="width:\${width}%"></div></div>
          <div class="bar-nums"><span class="bar-num">\${v.toLocaleString()}</span><span class="bar-pct">\${pct}%</span></div>
        </div>\`;
      }).join("")
    : '<div style="color:#94a3b8;font-size:12px;padding:10px 0">暂无数据</div>';

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
  const map = { hosts: ["tbHosts", "pagerHosts"], ips: ["tbIPs", "pagerIPs"], clients: ["tbClients", "pagerClients"] };
  renderTable(key, map[key][0], map[key][1]);
}
window.changePage = changePage;

function renderRegions() {
  const list = document.getElementById("tbRegions");
  const countries = regionCountries || [];
  const provinces = regionProvinces || [];
  if (!countries.length) {
    list.innerHTML = '<div style="color:#94a3b8;font-size:12px;padding:10px 0">暂无数据</div>';
    return;
  }
  const total = countries.reduce((s, x) => s + x[1], 0) || 1;
  const maxC = Math.max(...countries.map(x => x[1]), 1);
  const maxP = provinces.length ? Math.max(...provinces.map(x => x[1]), 1) : 1;

  let html = "";
  for (const [name, v] of countries) {
    const pct = Math.round(v / total * 100);
    const width = Math.max(2, (v / maxC) * 100);
    const canExpand = name === "中国" && provinces.length > 0;
    const arrow = canExpand ? (regionExpanded ? "▾ " : "▸ ") : "";
    const cls = canExpand ? "clickable" : "";
    html += \`<div class="bar-item">
      <div class="bar-label \${cls}" onclick="\${canExpand ? "toggleRegion()" : ""}">\${arrow}\${name}</div>
      <div class="bar-track"><div class="bar-fill" style="width:\${width}%"></div></div>
      <div class="bar-nums"><span class="bar-num">\${v.toLocaleString()}</span><span class="bar-pct">\${pct}%</span></div>
    </div>\`;
    if (canExpand && regionExpanded) {
      const pTotal = provinces.reduce((s, x) => s + x[1], 0) || 1;
      for (const [pn, pv] of provinces) {
        const pp = Math.round(pv / pTotal * 100);
        const pw = Math.max(2, (pv / maxP) * 100);
        html += \`<div class="bar-item sub">
          <div class="bar-label">\${pn}</div>
          <div class="bar-track"><div class="bar-fill sub" style="width:\${pw}%"></div></div>
          <div class="bar-nums"><span class="bar-num">\${pv.toLocaleString()}</span><span class="bar-pct">\${pp}%</span></div>
        </div>\`;
      }
    }
  }
  list.innerHTML = html;
}

function toggleRegion() {
  regionExpanded = !regionExpanded;
  renderRegions();
}
window.toggleRegion = toggleRegion;

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
  const loadingEl = document.getElementById("loading");
  loadingEl.style.display = "flex";
  loadingEl.textContent = "加载中...";
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
    loadingEl.textContent = "加载失败: " + e.message;
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
  tableState.clients = { data: data.top_clients || [], page: 0 };

  regionCountries = data.top_countries || [];
  regionProvinces = data.top_provinces || [];
  regionExpanded = false;

  renderTable("hosts", "tbHosts", "pagerHosts");
  renderTable("ips", "tbIPs", "pagerIPs");
  renderRegions();
  renderTable("clients", "tbClients", "pagerClients");

  document.getElementById("loading").style.display = "none";
  document.getElementById("content").style.display = "block";
}

loadNodes();
loadData();
</script>
</body>
</html>`;
