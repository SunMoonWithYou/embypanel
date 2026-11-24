/**
 * Emby Proxy 流量监控 Worker
 * 
 * 功能：
 * 1. 接收后台上报（/collect），按「节点+日期」聚合存储到 KV
 * 2. 去重：相同 batch_id 只处理一次
 * 3. 缓存：今天/昨天/近7天 的结果预计算，只有新数据才更新缓存
 * 4. 提供前端看板（/），支持节点筛选、时间范围、自定义日期
 * 5. 隐私处理：主机和 IP 脱敏显示
 * 
 * 需要绑定：
 * - KV 命名空间：REPORTS
 * - 可选环境变量：REPORT_TOKEN（上报鉴权）
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 跨域预检
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
      });
    }

    // 接收上报
    if (url.pathname === "/collect" && request.method === "POST") {
      return handleCollect(request, env);
    }

    // 查询统计数据
    if (url.pathname === "/api/stats" && request.method === "GET") {
      return handleStats(url, env);
    }

    // 查询节点列表
    if (url.pathname === "/api/nodes" && request.method === "GET") {
      return handleNodes(env);
    }

    // 前端页面
    if (url.pathname === "/" || url.pathname === "/index.html") {
      return new Response(HTML, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    return new Response("Not Found", { status: 404 });
  },
};

// ==================== 隐私脱敏工具 ====================

/** 对单个标签脱敏：首尾保留，中间变*，单字符直接* */
function privacyLabel(s) {
  if (!s) return s;
  const len = s.length;
  if (len <= 1) return "*";
  if (len === 2) return s[0] + "*";
  return s[0] + "*".repeat(len - 2) + s[len - 1];
}

/** 主机脱敏：按点分割，每段单独脱敏 */
function privacyHost(host) {
  if (!host) return host;
  host = host.split(":")[0].toLowerCase();
  return host.split(".").map(privacyLabel).join(".");
}

/** IP 脱敏：IPv4 保留前两段，IPv6 保留首尾 */
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

/** 统一返回 JSON */
function json(data) {
  return new Response(JSON.stringify(data), {
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  });
}

/** 合并计数 map，可选脱敏函数 */
function mergeMap(target, source, maskFn) {
  if (!source) return;
  for (const [k, v] of Object.entries(source)) {
    const key = maskFn ? maskFn(k) : k;
    target[key] = (target[key] || 0) + v;
  }
}

// ==================== 接收上报 ====================

/**
 * 处理上报：
 * 1. 鉴权（可选）
 * 2. 用 batch_id 去重
 * 3. 按节点+日期聚合到 agg:{node}:{date}
 * 4. 更新节点索引
 * 5. 刷新常用范围缓存
 */
async function handleCollect(request, env) {
  // 可选鉴权
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

  // 去重：相同 batch_id 直接跳过
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

  // 读取当天聚合对象
  const aggKey = `agg:${nodeKey}:${date}`;
  let agg = {};
  const raw = await env.REPORTS.get(aggKey);
  if (raw) {
    try { agg = JSON.parse(raw); } catch {}
  }

  // 初始化结构
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

  // 累加总量
  agg.requests += payload.requests || 0;
  agg.up_bytes += payload.up_bytes || 0;
  agg.down_bytes += payload.down_bytes || 0;
  agg.blocked += payload.blocked || 0;
  agg.note = nodeNote;

  // 小时明细（日报不写入）
  if (!isDaily) {
    const h = String(hour).padStart(2, "0");
    if (!agg.hours[h]) agg.hours[h] = { requests: 0, up: 0, down: 0, blocked: 0 };
    agg.hours[h].requests += payload.requests || 0;
    agg.hours[h].up += payload.up_bytes || 0;
    agg.hours[h].down += payload.down_bytes || 0;
    agg.hours[h].blocked += payload.blocked || 0;
  }

  // 合并排行（已脱敏）
  mergeMap(agg.hosts, payload.hosts, privacyHost);
  mergeMap(agg.ips, payload.ips, privacyIP);
  mergeMap(agg.countries, payload.countries);
  mergeMap(agg.provinces, payload.provinces);

  // 限制排行大小，避免单 key 过大
  function prune(map, n = 40) {
    const entries = Object.entries(map).sort((a, b) => b[1] - a[1]);
    if (entries.length <= n) return map;
    return Object.fromEntries(entries.slice(0, n));
  }
  agg.hosts = prune(agg.hosts, 30);
  agg.ips = prune(agg.ips, 40);
  agg.countries = prune(agg.countries, 20);
  agg.provinces = prune(agg.provinces, 30);

  // 写入聚合（保留 90 天）
  await env.REPORTS.put(aggKey, JSON.stringify(agg), {
    expirationTtl: 60 * 60 * 24 * 90,
  });

  // 标记已处理（7 天过期）
  await env.REPORTS.put(seenKey, "1", {
    expirationTtl: 60 * 60 * 24 * 7,
  });

  // 更新节点索引
  await updateNodeIndex(env, nodeKey, nodeNote);

  // 新数据到达，刷新常用缓存
  await updateReadyCache(env, "today");
  await updateReadyCache(env, "yesterday");
  await updateReadyCache(env, "7d");

  return new Response(JSON.stringify({ ok: true, batch_id: payload.batch_id }), {
    headers: { "Content-Type": "application/json" },
  });
}

/** 更新节点索引（用于前端下拉框） */
async function updateNodeIndex(env, nodeKey, note) {
  const raw = await env.REPORTS.get("nodes:index");
  let nodes = {};
  if (raw) {
    try { nodes = JSON.parse(raw); } catch {}
  }
  nodes[nodeKey] = { note, last_seen: new Date().toISOString() };
  await env.REPORTS.put("nodes:index", JSON.stringify(nodes));
}

/** 预计算并缓存常用范围（今天/昨天/7天） */
async function updateReadyCache(env, range) {
  try {
    const data = await computeStats(env, range, "");
    const key = `ready:${range}:all`;
    await env.REPORTS.put(key, JSON.stringify(data), {
      expirationTtl: 60 * 60 * 24 * 2,
    });
  } catch (e) {}
}

// ==================== API ====================

/** 返回节点列表 */
async function handleNodes(env) {
  if (!env.REPORTS) return json({ nodes: {} });
  const raw = await env.REPORTS.get("nodes:index");
  let nodes = {};
  if (raw) {
    try { nodes = JSON.parse(raw); } catch {}
  }
  return json({ nodes });
}

/**
 * 查询统计：
 * - 常用范围（今天/昨天/7天）+ 全部节点：优先读缓存
 * - 其他情况实时计算
 */
async function handleStats(url, env) {
  if (!env.REPORTS) {
    return json({ error: "KV not bound", summary: {}, series: [], top_hosts: [], top_ips: [], top_countries: [], top_provinces: [] });
  }

  const range = url.searchParams.get("range") || "today";
  const node = url.searchParams.get("node") || "";
  const customStart = url.searchParams.get("start") || "";
  const customEnd = url.searchParams.get("end") || "";

  // 优先读缓存
  if (!customStart && !node && ["today", "yesterday", "7d"].includes(range)) {
    const cached = await env.REPORTS.get(`ready:${range}:all`);
    if (cached) {
      try {
        return json(JSON.parse(cached));
      } catch {}
    }
  }

  // 实时计算
  const data = await computeStats(env, range, node, customStart, customEnd);
  return json(data);
}

/**
 * 核心统计计算：
 * 1. 根据范围算出日期列表
 * 2. 拉取对应节点+日期的聚合数据
 * 3. 汇总请求、流量、小时序列、排行
 * 4. 单天时补全 24 小时（缺的为 0）
 */
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

  // 生成日期列表
  const dates = [];
  let cur = new Date(startDate + "T00:00:00Z");
  const end = new Date(endDate + "T00:00:00Z");
  while (cur <= end) {
    dates.push(cur.toISOString().slice(0, 10));
    cur.setDate(cur.getDate() + 1);
  }

  // 确定要查的节点
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

  // 拉取聚合数据
  const aggs = [];
  for (const nk of nodeKeys) {
    for (const d of dates) {
      const raw = await env.REPORTS.get(`agg:${nk}:${d}`);
      if (raw) {
        try { aggs.push(JSON.parse(raw)); } catch {}
      }
    }
  }

  // 汇总
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

    // 小时序列
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

  // 生成图表序列
  const series = [];
  if (dates.length === 1) {
    // 单天：补全 24 小时
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
    // 多天：按有数据的小时显示
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

  // 排行
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

// ==================== 前端页面 ====================
const HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>流量监控</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='20' fill='%233b82f6'/><path d='M20 70 L35 45 L50 60 L65 30 L80 50' stroke='white' stroke-width='8' fill='none' stroke-linecap='round' stroke-linejoin='round'/></svg>">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg: #f0f4f8;
  --card: #ffffff;
  --bd: #e2e8f0;
  --t: #1e293b;
  --t2: #475569;
  --t3: #94a3b8;
  --p: #3b82f6;
  --shadow: 0 4px 12px -2px rgba(15, 23, 42, 0.08);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  background: linear-gradient(180deg, #f8fafc 0%, #f0f4f8 100%);
  color: var(--t);
  min-height: 100vh;
}
.header {
  background: rgba(255,255,255,0.95);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid var(--bd);
  padding: 12px 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 20;
  box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}
.header-left {
  display: flex;
  align-items: center;
  gap: 10px;
}
.header-left .icon {
  width: 32px;
  height: 32px;
  border-radius: 8px;
  background: #3b82f6;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
.header-left h1 { font-size: 17px; font-weight: 700; letter-spacing: -0.3px; }
.header-left .sub { font-size: 11px; color: var(--t3); margin-top: 1px; }
.node-select {
  padding: 6px 10px 6px 12px;
  border: 1px solid #d1d5db;
  border-radius: 8px;
  font-size: 13px;
  background: #fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%2364748b' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E") no-repeat right 8px center;
  background-size: 12px;
  appearance: none;
  min-width: 110px;
  color: var(--t);
  box-shadow: 0 1px 2px rgba(0,0,0,0.04);
  cursor: pointer;
}
.node-select:hover { border-color: #94a3b8; }
.node-select:focus { outline: none; border-color: var(--p); box-shadow: 0 0 0 3px rgba(59,130,246,0.15); }
.container { padding: 14px 12px 32px; max-width: 1600px; margin: 0 auto; }
.filter-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 14px;
}
.tabs {
  display: flex;
  gap: 4px;
  background: #fff;
  padding: 4px;
  border-radius: 10px;
  border: 1px solid var(--bd);
  box-shadow: var(--shadow);
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  scrollbar-width: none;
  max-width: 100%;
}
.tabs::-webkit-scrollbar { display: none; }
.tab {
  padding: 6px 12px;
  border-radius: 7px;
  font-size: 13px;
  cursor: pointer;
  color: var(--t2);
  border: none;
  background: transparent;
  transition: all 0.15s;
  white-space: nowrap;
  flex-shrink: 0;
}
.tab.active { background: var(--p); color: #fff; font-weight: 600; }
.date-filter {
  display: none;
  align-items: center;
  gap: 6px;
  background: #fff;
  padding: 4px 8px;
  border-radius: 10px;
  border: 1px solid var(--bd);
  box-shadow: var(--shadow);
}
.date-filter.show { display: flex; }
.date-filter input[type="date"] {
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  padding: 5px 8px;
  font-size: 13px;
  color: var(--t);
}
.date-filter button {
  background: var(--p);
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 6px 12px;
  font-size: 13px;
  cursor: pointer;
  font-weight: 500;
}
.date-filter button:hover { background: #2563eb; }
.cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  margin-bottom: 14px;
}
@media (max-width: 1100px) { .cards { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 600px) { .cards { grid-template-columns: 1fr 1fr; } }
.card {
  background: var(--card);
  border: 1px solid var(--bd);
  border-radius: 12px;
  padding: 14px 16px;
  position: relative;
  box-shadow: var(--shadow);
  transition: transform 0.15s, box-shadow 0.15s;
}
.card:hover { transform: translateY(-2px); box-shadow: 0 8px 20px -4px rgba(15,23,42,0.12); }
.card::before {
  content: "";
  position: absolute;
  left: 0; top: 0; bottom: 0;
  width: 4px;
  border-radius: 12px 0 0 12px;
}
.card.req::before { background: linear-gradient(180deg, #3b82f6, #60a5fa); }
.card.traffic::before { background: linear-gradient(180deg, #10b981, #34d399); }
.card.ip::before { background: linear-gradient(180deg, #f59e0b, #fbbf24); }
.card.block::before { background: linear-gradient(180deg, #ef4444, #f87171); }
.card .label { font-size: 12px; color: var(--t3); margin-bottom: 4px; font-weight: 500; }
.card .value { font-size: 22px; font-weight: 750; letter-spacing: -0.5px; }
.card .sub { font-size: 11px; color: var(--t2); margin-top: 4px; }
.chart-card {
  background: var(--card);
  border: 1px solid var(--bd);
  border-radius: 12px;
  padding: 14px 14px 10px;
  margin-bottom: 12px;
  box-shadow: var(--shadow);
}
.chart-card h3 { font-size: 14px; font-weight: 650; margin-bottom: 2px; }
.chart-card .desc { font-size: 11.5px; color: var(--t3); margin-bottom: 8px; }
.chart-wrap { height: 180px; position: relative; }
.tables {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  align-items: stretch;
}
@media (max-width: 1200px) { .tables { grid-template-columns: repeat(2, 1fr); } }
@media (max-width: 680px) { .tables { grid-template-columns: 1fr; } }
.table-card {
  background: var(--card);
  border: 1px solid var(--bd);
  border-radius: 12px;
  padding: 12px 14px 10px;
  box-shadow: var(--shadow);
  display: flex;
  flex-direction: column;
  height: 100%;
}
.table-card h3 { font-size: 13px; font-weight: 650; margin-bottom: 8px; color: var(--t); flex-shrink: 0; }
.table-card table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
th, td { text-align: left; padding: 5px 4px; border-bottom: 1px solid #f1f5f9; }
th { color: var(--t3); font-weight: 500; font-size: 11.5px; }
tr:last-child td { border-bottom: none; }
.pager {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  margin-top: auto;
  padding-top: 10px;
  font-size: 12px;
  color: var(--t3);
  white-space: nowrap;
  border-top: 1px solid #f1f5f9;
  flex-shrink: 0;
}
.pager button {
  padding: 3px 8px;
  border: 1px solid var(--bd);
  background: #fff;
  border-radius: 5px;
  cursor: pointer;
  font-size: 11.5px;
  color: var(--t2);
  min-width: 52px;
}
.pager button:disabled { opacity: 0.35; cursor: not-allowed; }
.pager button:hover:not(:disabled) { background: #f8fafc; }
.pager .page-info { min-width: 48px; text-align: center; font-variant-numeric: tabular-nums; }
.loading { text-align: center; padding: 60px 20px; color: var(--t3); font-size: 14px; }
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <div class="icon">
      <svg width="20" height="20" viewBox="0 0 100 100" fill="none">
        <path d="M15 75 L32 42 L48 58 L65 28 L85 52" stroke="white" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    </div>
    <div>
      <h1>流量监控</h1>
      <div class="sub">Emby Proxy 数据看板</div>
    </div>
  </div>
  <select id="nodeSelect" class="node-select"><option value="">全部节点</option></select>
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
// ========== 前端逻辑 ==========
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

// 初始化日期选择器
const today = new Date().toISOString().slice(0, 10);
document.getElementById("startDate").value = today;
document.getElementById("endDate").value = today;

// 标签切换
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

// 节点切换
document.getElementById("nodeSelect").addEventListener("change", e => {
  currentNode = e.target.value;
  loadData();
});

// 自定义日期查询
function loadCustom() {
  const s = document.getElementById("startDate").value;
  const e = document.getElementById("endDate").value;
  if (!s || !e) return alert("请选择开始和结束日期");
  if (s > e) return alert("开始日期不能晚于结束日期");
  customStart = s;
  customEnd = e;
  loadData();
}

// 字节格式化
function formatBytes(b) {
  if (b >= 1e12) return (b / 1e12).toFixed(2) + " TB";
  if (b >= 1e9) return (b / 1e9).toFixed(2) + " GB";
  if (b >= 1e6) return (b / 1e6).toFixed(2) + " MB";
  return (b / 1e3).toFixed(1) + " KB";
}

// 渲染单个表格 + 分页
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

// 翻页
function changePage(key, delta) {
  const state = tableState[key];
  const totalPages = Math.max(1, Math.ceil(state.data.length / PAGE_SIZE));
  state.page = Math.max(0, Math.min(totalPages - 1, state.page + delta));
  const map = { hosts: ["tbHosts", "pagerHosts"], ips: ["tbIPs", "pagerIPs"], countries: ["tbCountries", "pagerCountries"], provinces: ["tbProvinces", "pagerProvinces"] };
  renderTable(key, map[key][0], map[key][1]);
}
window.changePage = changePage;

// 加载节点列表
async function loadNodes() {
  try {
    const r = await fetch("/api/nodes");
    const data = await r.json();
    const sel = document.getElementById("nodeSelect");
    Object.entries(data.nodes || {}).forEach(([k, v]) => {
      const opt = document.createElement("option");
      opt.value = k;
      opt.textContent = v.note || k;
      sel.appendChild(opt);
    });
  } catch (e) {}
}

// 加载统计数据
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

// 渲染页面
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

  // 流量图
  if (trafficChart) trafficChart.destroy();
  trafficChart = new Chart(document.getElementById("trafficChart"), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "流量 (GB)",
        data: trafficData,
        borderColor: "#10b981",
        backgroundColor: "rgba(16,185,129,0.18)",
        fill: true,
        tension: 0.35,
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
          callbacks: {
            label: function(ctx) {
              return "流量: " + ctx.parsed.y + " GB";
            }
          }
        }
      },
      scales: {
        y: { beginAtZero: true, grid: { color: "#f1f5f9" } },
        x: { grid: { display: false } }
      }
    }
  });

  // 请求图
  if (reqChart) reqChart.destroy();
  reqChart = new Chart(document.getElementById("reqChart"), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "请求数",
        data: reqData,
        borderColor: "#3b82f6",
        backgroundColor: "rgba(59,130,246,0.18)",
        fill: true,
        tension: 0.35,
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
          callbacks: {
            label: function(ctx) {
              return "请求: " + ctx.parsed.y.toLocaleString();
            }
          }
        }
      },
      scales: {
        y: { beginAtZero: true, grid: { color: "#f1f5f9" } },
        x: { grid: { display: false } }
      }
    }
  });

  // 表格数据
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

// 初始化
loadNodes();
loadData();
</script>
</body>
</html>`;
