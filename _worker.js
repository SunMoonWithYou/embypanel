/**
 * Emby Proxy 流量监控 Worker（完整注释版）
 * 
 * 功能：
 * - 接收客户端上报的流量数据，按月聚合存储到 KV
 * - 提供前端看板，展示请求量、流量、IP、地区、播放器等统计
 * - 隐私化在展示时处理，存储保留原始数据
 * 
 * KV 结构：
 * - agg:{nodeKey}:{YYYY-MM}  月度聚合数据，内含每日详情
 * - seen:{batch_id}           去重标记，7 天过期
 * - nodes:index               节点索引 {nodeKey: {note, last_seen}}
 * - cache:{range}             历史查询缓存，5 分钟过期
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS 预检
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
      });
    }

    // 数据上报接口
    if (url.pathname === "/collect" && request.method === "POST") {
      return handleCollect(request, env);
    }
    // 统计查询接口
    if (url.pathname === "/api/stats" && request.method === "GET") {
      return handleStats(url, env);
    }
    // 节点列表接口
    if (url.pathname === "/api/nodes" && request.method === "GET") {
      return handleNodes(env);
    }
    // 前端页面
    if (url.pathname === "/" || url.pathname === "/index.html") {
      return new Response(HTML, { headers: { "Content-Type": "text/html; charset=utf-8" } });
    }
    return new Response("Not Found", { status: 404 });
  },
};

// ==================== 隐私化函数（仅展示时调用） ====================

/**
 * 对字符串做隐私化：首尾保留，中间用 * 替换
 * 长度 <= 1 直接返回 *
 * 长度 = 2 返回首字符 + *
 */
function privacyLabel(s) {
  if (!s) return s;
  const len = s.length;
  if (len <= 1) return "*";
  if (len === 2) return s[0] + "*";
  return s[0] + "*".repeat(len - 2) + s[len - 1];
}

/**
 * 对域名做隐私化：按 . 分割，每段单独处理
 * 例：example.com -> e*****e.c*m
 */
function privacyHost(host) {
  if (!host) return host;
  return host.split(":")[0].toLowerCase().split(".").map(privacyLabel).join(".");
}

/**
 * 主机聚合 key：去掉端口，只按域名合并
 * 例：example.com:8443 -> example.com
 * 兼容 IPv6 字面量：[::1]:443 -> [::1]
 */
function hostKey(host) {
  if (!host) return host;
  const h = String(host).trim().toLowerCase();
  if (h.startsWith("[")) {
    const end = h.indexOf("]");
    return end === -1 ? h : h.slice(0, end + 1);
  }
  const idx = h.indexOf(":");
  return idx === -1 ? h : h.slice(0, idx);
}

/**
 * 对 IP 做隐私化
 * IPv4：保留前两段，后两段变 *. *
 * IPv6：保留首尾段，中间变 ****
 */
function privacyIP(ip) {
  if (!ip) return ip;
  if (ip.includes(".") && !ip.includes(":")) {
    const p = ip.split(".");
    if (p.length === 4) return `${p[0]}.${p[1]}.*.*`;
  }
  if (ip.includes(":")) {
    const p = ip.split(":");
    if (p.length > 2) return p[0] + ":****:" + p[p.length - 1];
  }
  return ip;
}

// ==================== 工具函数 ====================

/** 返回 JSON 响应，带 CORS 头 */
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  });
}

/**
 * 合并两个 map，可选地对 key 做转换
 * target[key] += source[key]
 */
function mergeMap(target, source, maskFn) {
  if (!source) return;
  for (const [k, v] of Object.entries(source)) {
    const key = maskFn ? maskFn(k) : k;
    target[key] = (target[key] || 0) + v;
  }
}

/** 小时标准化：补零成两位，无效值返回 "daily" */
function normHour(h) {
  if (h == null || h === "" || h === "daily") return "daily";
  const n = parseInt(String(h), 10);
  return isNaN(n) ? "daily" : String(n).padStart(2, "0");
}

/** 保留 map 中值最大的前 n 项 */
function prune(map, n) {
  const entries = Object.entries(map).sort((a, b) => b[1] - a[1]);
  return Object.fromEntries(entries.slice(0, n));
}

// ==================== 数据上报 ====================

/**
 * 处理客户端上报
 * 
 * 流程：
 * 1. 校验 token 和必填字段
 * 2. 检查 batch_id 去重
 * 3. 读取月度聚合数据，累加本次上报
 * 4. 写入月度数据、去重标记、节点索引
 * 
 * 存储格式 agg:{nodeKey}:{YYYY-MM}：
 * {
 *   "2026-11-24": {
 *     date, node_key, note, requests, up_bytes, down_bytes, blocked,
 *     hours: { "13": { requests, up, down, blocked }, ... },
 *     hosts, ips, countries, provinces, clients
 *   },
 *   ...
 * }
 */
async function handleCollect(request, env) {
  // token 校验（可选）
  const token = env.REPORT_TOKEN || "";
  if (token && request.headers.get("Authorization") !== `Bearer ${token}`) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }

  let payload;
  try { payload = await request.json(); } catch { return json({ ok: false, error: "invalid json" }, 400); }
  if (!payload.batch_id || !payload.period_date) return json({ ok: false, error: "missing fields" }, 400);
  if (!env.REPORTS) return json({ ok: false, error: "KV not bound" }, 500);

  // 去重检查
  const seenKey = `seen:${payload.batch_id}`;
  if (await env.REPORTS.get(seenKey)) {
    return json({ ok: true, batch_id: payload.batch_id, duplicate: true });
  }

  const nodeKey = payload.node_key || "unknown";
  const nodeNote = payload.note || payload.node || nodeKey;
  const date = payload.period_date;
  const month = date.slice(0, 7); // YYYY-MM
  const hour = normHour(payload.period_hour);
  const isDaily = hour === "daily";

  // 读取月度聚合
  const aggKey = `agg:${nodeKey}:${month}`;
  let aggMonth = {};
  const raw = await env.REPORTS.get(aggKey);
  if (raw) { try { aggMonth = JSON.parse(raw); } catch {} }

  // 取当天数据，不存在则初始化
  let agg = aggMonth[date] || {
    date, node_key: nodeKey, note: nodeNote,
    requests: 0, up_bytes: 0, down_bytes: 0, blocked: 0,
    hours: {}, hosts: {}, ips: {}, countries: {}, provinces: {}, clients: {},
  };
  if (!agg.hours) agg.hours = {};
  if (!agg.clients) agg.clients = {};

  // 累加总量
  agg.requests += payload.requests || 0;
  agg.up_bytes += payload.up_bytes || 0;
  agg.down_bytes += payload.down_bytes || 0;
  agg.blocked += payload.blocked || 0;
  agg.note = nodeNote;

  // 累加小时数据（非日报）
  if (!isDaily) {
    if (!agg.hours[hour]) agg.hours[hour] = { requests: 0, up: 0, down: 0, blocked: 0 };
    agg.hours[hour].requests += payload.requests || 0;
    agg.hours[hour].up += payload.up_bytes || 0;
    agg.hours[hour].down += payload.down_bytes || 0;
    agg.hours[hour].blocked += payload.blocked || 0;
  }

  // 合并明细（存储时不隐私化；hosts 按域名合并，忽略端口）
  mergeMap(agg.hosts, payload.hosts, hostKey);
  mergeMap(agg.ips, payload.ips);
  mergeMap(agg.countries, payload.countries);
  mergeMap(agg.provinces, payload.provinces);
  mergeMap(agg.clients, payload.clients);

  // 裁剪，保留 top N，控制 KV 体积
  agg.hosts = prune(agg.hosts, 30);
  agg.ips = prune(agg.ips, 40);
  agg.countries = prune(agg.countries, 20);
  agg.provinces = prune(agg.provinces, 30);
  agg.clients = prune(agg.clients, 30);

  aggMonth[date] = agg;

  // 并行写入
  const writes = [
    env.REPORTS.put(aggKey, JSON.stringify(aggMonth), { expirationTtl: 60 * 60 * 24 * 90 }), // 90 天过期
    env.REPORTS.put(seenKey, "1", { expirationTtl: 60 * 60 * 24 * 7 }) // 去重标记 7 天过期
  ];

  // 节点索引：每小时最多更新一次
  const nodeIdxKey = "nodes:index";
  const nodesRaw = await env.REPORTS.get(nodeIdxKey);
  let nodes = {};
  if (nodesRaw) { try { nodes = JSON.parse(nodesRaw); } catch {} }
  const lastSeen = nodes[nodeKey]?.last_seen;
  const shouldUpdate = !lastSeen || (Date.now() - new Date(lastSeen).getTime() > 3600000);
  if (shouldUpdate || nodes[nodeKey]?.note !== nodeNote) {
    nodes[nodeKey] = { note: nodeNote, last_seen: new Date().toISOString() };
    writes.push(env.REPORTS.put(nodeIdxKey, JSON.stringify(nodes)));
  }

  await Promise.all(writes);

  return json({ ok: true, batch_id: payload.batch_id });
}

// ==================== 节点列表 ====================

/** 返回节点列表，索引为空时自动从 KV 扫描重建 */
async function handleNodes(env) {
  if (!env.REPORTS) return json({ nodes: {} });
  let nodes = {};
  const raw = await env.REPORTS.get("nodes:index");
  if (raw) { try { nodes = JSON.parse(raw); } catch {} }
  
  if (Object.keys(nodes).length === 0) {
    nodes = await rebuildNodeIndex(env);
  }
  return json({ nodes });
}

/** 扫描 agg: 前缀的 key，重建节点索引 */
async function rebuildNodeIndex(env) {
  const nodes = {};
  try {
    const list = await env.REPORTS.list({ prefix: "agg:" });
    for (const key of list.keys) {
      const parts = key.name.split(":");
      if (parts.length >= 3 && !nodes[parts[1]]) {
        try {
          const raw = await env.REPORTS.get(key.name);
          if (raw) {
            const data = JSON.parse(raw);
            const first = Object.values(data)[0];
            nodes[parts[1]] = { note: first?.note || parts[1].slice(0, 8), last_seen: new Date().toISOString() };
          }
        } catch {
          nodes[parts[1]] = { note: parts[1].slice(0, 8), last_seen: new Date().toISOString() };
        }
      }
    }
    if (Object.keys(nodes).length > 0) {
      await env.REPORTS.put("nodes:index", JSON.stringify(nodes));
    }
  } catch {}
  return nodes;
}

// ==================== 统计查询 ====================

/**
 * 处理统计查询
 * 
 * 缓存策略：
 * - today：不缓存，每次实时计算
 * - yesterday / 7d / 30d：缓存 5 分钟
 * - custom：不缓存
 */
async function handleStats(url, env) {
  if (!env.REPORTS) return json({ error: "KV not bound", summary: {}, series: [], top_hosts: [], top_ips: [], top_countries: [], top_provinces: [], top_clients: [] });

  const range = url.searchParams.get("range") || "today";
  const node = url.searchParams.get("node") || "";
  const customStart = url.searchParams.get("start") || "";
  const customEnd = url.searchParams.get("end") || "";

  // 仅历史范围且无节点筛选时缓存
  const canCache = !customStart && !node && ["yesterday", "7d", "30d"].includes(range);
  const cacheKey = `cache:${range}`;

  if (canCache) {
    const cached = await env.REPORTS.get(cacheKey);
    if (cached) {
      try { return json(JSON.parse(cached)); } catch {}
    }
  }

  const data = await computeStats(env, range, node, customStart, customEnd);

  if (canCache) {
    env.REPORTS.put(cacheKey, JSON.stringify(data), { expirationTtl: 300 }).catch(() => {});
  }

  return json(data);
}

/**
 * 计算统计数据
 * 
 * 单日（today / yesterday / 自定义同一天）：返回 24 小时序列
 * 多日（7d / 30d / 自定义区间）：返回按小时聚合的序列
 * 
 * 返回的 top_hosts / top_ips 已隐私化
 */
async function computeStats(env, range, node, customStart = "", customEnd = "") {
  // 计算北京时间
  const now = new Date();
  const bj = new Date(now.getTime() + 8 * 3600 * 1000);
  const formatDate = (d) => d.toISOString().slice(0, 10);

  // 确定查询日期范围
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
    // all：最近 90 天
    const s = new Date(bj); s.setDate(s.getDate() - 89);
    startDate = formatDate(s);
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

  const months = [...new Set(dates.map(d => d.slice(0, 7)))];

  // 获取节点列表
  let nodeKeys = [];
  if (node) {
    nodeKeys = [node];
  } else {
    const raw = await env.REPORTS.get("nodes:index");
    if (raw) { try { nodeKeys = Object.keys(JSON.parse(raw)); } catch {} }
    if (nodeKeys.length === 0) {
      const scanned = await rebuildNodeIndex(env);
      nodeKeys = Object.keys(scanned);
    }
  }

  // 并行读取所有月度数据
  const readTasks = [];
  for (const nk of nodeKeys) {
    for (const m of months) {
      readTasks.push(env.REPORTS.get(`agg:${nk}:${m}`));
    }
  }
  const results = await Promise.all(readTasks);

  // 提取有效日期的聚合数据
  const aggs = [];
  const validDates = new Set(dates);
  let idx = 0;
  for (const nk of nodeKeys) {
    for (const m of months) {
      const raw = results[idx++];
      if (raw) {
        try {
          const monthData = JSON.parse(raw);
          for (const [d, agg] of Object.entries(monthData)) {
            if (validDates.has(d) && agg && typeof agg === "object") {
              if (!agg.date) agg.date = d;
              aggs.push(agg);
            }
          }
        } catch {}
      }
    }
  }

  // 汇总统计
  let totalRequests = 0, totalUp = 0, totalDown = 0, totalBlocked = 0;
  const ipSet = new Set();
  const hourly = {};
  const hosts = {}, countries = {}, provinces = {}, clients = {}, topIPs = {};

  for (const a of aggs) {
    totalRequests += a.requests || 0;
    totalUp += a.up_bytes || 0;
    totalDown += a.down_bytes || 0;
    totalBlocked += a.blocked || 0;

    // 汇总小时数据
    if (a.hours) {
      for (const [h, v] of Object.entries(a.hours)) {
        if (typeof v !== "object" || !v) continue;
        const hh = normHour(h);
        if (hh === "daily") continue;
        const key = `${a.date} ${hh}`;
        if (!hourly[key]) hourly[key] = { requests: 0, up: 0, down: 0, blocked: 0 };
        hourly[key].requests += v.requests || 0;
        hourly[key].up += v.up || 0;
        hourly[key].down += v.down || 0;
        hourly[key].blocked += v.blocked || 0;
      }
    }

    mergeMap(hosts, a.hosts, hostKey); // 按域名合并，忽略端口（兼容历史已带端口的数据）
    mergeMap(topIPs, a.ips);
    mergeMap(countries, a.countries);
    mergeMap(provinces, a.provinces);
    mergeMap(clients, a.clients);
    if (a.ips) for (const ip of Object.keys(a.ips)) ipSet.add(ip);
  }

  // 构建时间序列
  const series = [];
  if (dates.length === 1) {
    // 单日：补齐 24 小时，无数据的为 0
    const d = dates[0];
    for (let h = 0; h < 24; h++) {
      const hh = String(h).padStart(2, "0");
      const v = hourly[`${d} ${hh}`] || { requests: 0, up: 0, down: 0, blocked: 0 };
      series.push({ label: `${hh}:00`, requests: v.requests, up: v.up, down: v.down, traffic: v.up + v.down, blocked: v.blocked });
    }
  } else {
    // 多日：按时间排序输出
    Object.entries(hourly).sort(([a], [b]) => a.localeCompare(b)).forEach(([k, v]) => {
      series.push({ label: k.slice(5), requests: v.requests, up: v.up, down: v.down, traffic: v.up + v.down, blocked: v.blocked });
    });
  }

  // 隐私化后返回
  const topHosts = Object.entries(hosts).sort((a, b) => b[1] - a[1]).slice(0, 50)
    .map(([k, v]) => [privacyHost(k), v]);
  const topIPList = Object.entries(topIPs).sort((a, b) => b[1] - a[1]).slice(0, 50)
    .map(([k, v]) => [privacyIP(k), v]);

  return {
    range: customStart ? "custom" : range,
    node: node || "all",
    startDate, endDate,
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
    top_countries: Object.entries(countries).sort((a, b) => b[1] - a[1]).slice(0, 30),
    top_provinces: Object.entries(provinces).sort((a, b) => b[1] - a[1]).slice(0, 40),
    top_clients: Object.entries(clients).sort((a, b) => b[1] - a[1]).slice(0, 40),
    report_count: aggs.length,
  };
}

// ==================== 前端页面 ====================

const HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>流量看板</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='22' fill='%233b82f6'/><path d='M50 55 L72 33' stroke='white' stroke-width='10' stroke-linecap='round'/><path d='M22 78 A 34 34 0 1 1 78 78' stroke='white' stroke-width='10' fill='none' stroke-linecap='round'/></svg>">
<script>(function(){try{var t=localStorage.getItem("theme");if(!t)t=(window.matchMedia&&window.matchMedia("(prefers-color-scheme: dark)").matches)?"dark":"light";document.documentElement.setAttribute("data-theme",t);}catch(e){}})();</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{
  --bg:#f5f7fb; --bg2:#eef2f9;
  --card:rgba(255,255,255,.76); --card-solid:#ffffff;
  --header-bg:rgba(255,255,255,.72);
  --bd:rgba(15,23,42,.07); --bd2:#e7edf6;
  --t:#0f172a; --t2:#64748b; --t3:#94a3b8;
  --brand:#3b82f6; --ok:#10b981; --warn:#f59e0b; --danger:#ef4444;
  --track:#eef2f7; --hover:rgba(59,130,246,.06);
  --shadow:0 1px 2px rgba(15,23,42,.04),0 12px 28px -18px rgba(15,23,42,.35);
  --shadow-hover:0 2px 6px rgba(15,23,42,.05),0 20px 38px -20px rgba(15,23,42,.4);
  --radius:16px;
}
[data-theme="dark"]{
  --bg:#070c16; --bg2:#0b1220;
  --card:rgba(20,28,45,.72); --card-solid:#0f172a;
  --header-bg:rgba(7,12,22,.74);
  --bd:rgba(148,163,184,.13); --bd2:#1e293b;
  --t:#e8eefb; --t2:#94a3b8; --t3:#64748b;
  --track:#1b2537; --hover:rgba(96,165,250,.09);
  --shadow:0 1px 2px rgba(0,0,0,.4),0 16px 32px -20px rgba(0,0,0,.85);
  --shadow-hover:0 2px 8px rgba(0,0,0,.5),0 24px 44px -20px rgba(0,0,0,.95);
}
*{box-sizing:border-box;margin:0;padding:0}
html{-webkit-text-size-adjust:100%}
body{
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  color:var(--t);min-height:100vh;
  background:
    radial-gradient(1200px 520px at 10% -10%,rgba(59,130,246,.14),transparent 62%),
    radial-gradient(900px 440px at 90% -8%,rgba(16,185,129,.12),transparent 62%),
    linear-gradient(180deg,var(--bg2),var(--bg));
  background-attachment:fixed;
  -webkit-font-smoothing:antialiased;
  transition:background .3s,color .3s;
}
[data-theme="dark"] body{
  background:
    radial-gradient(1200px 520px at 10% -10%,rgba(59,130,246,.16),transparent 62%),
    radial-gradient(900px 440px at 90% -8%,rgba(16,185,129,.10),transparent 62%),
    linear-gradient(180deg,var(--bg2),var(--bg));
  background-attachment:fixed;
}
::selection{background:rgba(59,130,246,.22)}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:99px;border:2px solid transparent;background-clip:content-box}
::-webkit-scrollbar-thumb:hover{background:#cbd5e1;background-clip:content-box}

/* ---------- 顶栏 ---------- */
.header{
  position:sticky;top:0;z-index:30;
  display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;
  padding:12px 24px;
  background:var(--header-bg);
  backdrop-filter:blur(18px) saturate(180%);
  -webkit-backdrop-filter:blur(18px) saturate(180%);
  border-bottom:1px solid var(--bd);
}
.brand{display:flex;align-items:center;gap:12px}
.logo{
  width:38px;height:38px;border-radius:12px;flex:none;display:grid;place-items:center;
  background:linear-gradient(135deg,#3b82f6,#6366f1 55%,#8b5cf6);
  box-shadow:0 10px 20px -10px rgba(79,124,247,.95);
  transition:transform .25s cubic-bezier(.2,.8,.2,1);
}
.brand:hover .logo{transform:scale(1.06) rotate(-4deg)}
.brand-text h1{font-size:16px;font-weight:700;letter-spacing:.5px;background:linear-gradient(135deg,var(--t),var(--t2));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
.header-actions{display:flex;align-items:center;gap:8px}

.icon-btn{
  width:36px;height:36px;flex:none;display:grid;place-items:center;
  border:1px solid var(--bd2);background:var(--card-solid);color:var(--t2);
  border-radius:10px;cursor:pointer;transition:.18s;
}
.icon-btn:hover{color:var(--brand);border-color:#93c5fd;box-shadow:0 0 0 3px rgba(59,130,246,.12)}
[data-theme="light"] .ico-sun{display:none}
[data-theme="dark"] .ico-moon{display:none}

.node-dropdown{position:relative}
.node-btn{
  display:flex;align-items:center;justify-content:space-between;gap:10px;
  min-width:142px;max-width:230px;
  padding:8px 12px;border-radius:10px;
  border:1px solid var(--bd2);background:var(--card-solid);color:var(--t);
  font-size:13px;font-weight:500;font-family:inherit;cursor:pointer;transition:.18s;
}
.node-btn:hover{border-color:#93c5fd;box-shadow:0 0 0 3px rgba(59,130,246,.12)}
.node-btn span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.chev{flex:none;color:var(--t3);transition:transform .2s}
.node-dropdown.open .chev{transform:rotate(180deg)}
.node-menu{
  position:absolute;top:calc(100% + 8px);right:0;z-index:40;
  min-width:100%;max-width:260px;max-height:320px;overflow:auto;padding:6px;
  background:var(--card-solid);border:1px solid var(--bd2);border-radius:14px;
  box-shadow:0 22px 44px -18px rgba(15,23,42,.42);
  opacity:0;transform:translateY(-6px) scale(.98);pointer-events:none;
  transition:opacity .16s ease,transform .16s ease;
}
.node-menu.show{opacity:1;transform:none;pointer-events:auto}
.node-item{
  position:relative;
  padding:9px 6px;border-radius:9px;font-size:13px;color:var(--t2);cursor:pointer;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;transition:.15s;
}
.node-item:hover{background:var(--hover);color:var(--t);padding-left:9px}
.node-item.active{
  background:linear-gradient(135deg,rgba(59,130,246,.16),rgba(99,102,241,.16));
  color:#2563eb;font-weight:600;padding-right:22px;
}
.node-item.active::after{
  content:"✓";position:absolute;right:8px;top:50%;transform:translateY(-50%);
  font-size:11px;font-weight:700;opacity:.85;
}
[data-theme="dark"] .node-item.active{color:#93c5fd}

/* ---------- 容器 / 工具栏 ---------- */
.container{padding:18px 24px 40px;max-width:1600px;margin:0 auto}
.toolbar{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:16px}
.tabs{
  display:flex;gap:2px;padding:4px;border-radius:12px;
  background:var(--card-solid);border:1px solid var(--bd2);box-shadow:var(--shadow);
  overflow-x:auto;scrollbar-width:none;max-width:100%;
}
.tabs::-webkit-scrollbar{display:none}
.tab{
  padding:7px 14px;border:none;background:transparent;border-radius:9px;
  font-size:13px;font-weight:500;font-family:inherit;color:var(--t2);
  cursor:pointer;white-space:nowrap;transition:.18s;
}
.tab:hover{color:var(--t);background:var(--hover)}
.tab:active{transform:scale(.96)}
.tab.active{
  color:#fff;font-weight:600;
  background:linear-gradient(135deg,#3b82f6,#4f7cf7);
  box-shadow:0 8px 16px -10px rgba(59,130,246,1);
}
.date-filter{
  display:none;align-items:center;gap:8px;
  padding:5px 6px 5px 10px;border-radius:12px;
  background:var(--card-solid);border:1px solid var(--bd2);box-shadow:var(--shadow);
}
.date-filter.show{display:flex;flex-wrap:wrap}
.date-filter .sep{color:var(--t3);font-size:12px}
.date-filter input[type="date"]{
  border:1px solid var(--bd2);background:transparent;color:var(--t);
  border-radius:8px;padding:6px 8px;font-size:12px;font-family:inherit;outline:none;transition:.18s;
}
.date-filter input[type="date"]:focus{border-color:#93c5fd;box-shadow:0 0 0 3px rgba(59,130,246,.14)}
.btn-primary{
  padding:7px 14px;border:none;border-radius:9px;color:#fff;
  background:linear-gradient(135deg,#3b82f6,#4f7cf7);
  font-size:12.5px;font-weight:600;font-family:inherit;cursor:pointer;transition:.18s;
}
.btn-primary:hover{filter:brightness(1.07);box-shadow:0 10px 20px -12px rgba(59,130,246,1);transform:translateY(-1px)}
.btn-primary:active{transform:translateY(0) scale(.97)}

/* ---------- 统计卡片 ---------- */
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:16px}
.card{
  position:relative;overflow:hidden;
  padding:16px 18px;border-radius:var(--radius);
  background:var(--card);border:1px solid var(--bd);
  box-shadow:var(--shadow);
  backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
  transition:transform .22s cubic-bezier(.2,.8,.2,1),box-shadow .22s;
}
.card:hover{transform:translateY(-3px);box-shadow:var(--shadow-hover)}
.card::before{content:"";position:absolute;left:0;top:14px;bottom:14px;width:3px;border-radius:0 3px 3px 0;transition:width .22s}
.card:hover::before{width:4px}
.card.req::before{background:linear-gradient(#3b82f6,#60a5fa)}
.card.traffic::before{background:linear-gradient(#10b981,#34d399)}
.card.ip::before{background:linear-gradient(#f59e0b,#fbbf24)}
.card.block::before{background:linear-gradient(#ef4444,#f87171)}
.card-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.card-label{font-size:12px;font-weight:500;color:var(--t3);letter-spacing:.3px}
.card-ico{width:28px;height:28px;border-radius:9px;display:grid;place-items:center;transition:transform .25s cubic-bezier(.2,.8,.2,1)}
.card:hover .card-ico{transform:scale(1.1) rotate(-4deg)}
.card.req .card-ico{background:rgba(59,130,246,.12);color:#3b82f6}
.card.traffic .card-ico{background:rgba(16,185,129,.12);color:#10b981}
.card.ip .card-ico{background:rgba(245,158,11,.14);color:#f59e0b}
.card.block .card-ico{background:rgba(239,68,68,.12);color:#ef4444}
.card-value{
  font-size:26px;font-weight:750;line-height:1.15;letter-spacing:-.5px;
  font-variant-numeric:tabular-nums;
}
.card-sub{font-size:11.5px;color:var(--t2);margin-top:6px}

/* ---------- 图表 ---------- */
.chart-card{
  padding:16px 18px 10px;margin-bottom:16px;
  border-radius:var(--radius);
  background:var(--card);border:1px solid var(--bd);box-shadow:var(--shadow);
  backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
}
.card-hd{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:10px;flex-wrap:wrap}
.card-hd h3{font-size:14px;font-weight:650;letter-spacing:.2px}
.card-hd .desc{font-size:11.5px;color:var(--t3)}
.chart-wrap{height:180px;position:relative}

/* ---------- 榜单 ---------- */
.tables{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
.table-card{
  display:flex;flex-direction:column;
  padding:16px;border-radius:var(--radius);
  background:var(--card);border:1px solid var(--bd);box-shadow:var(--shadow);
  backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
}
.table-card h3{font-size:14px;font-weight:650;letter-spacing:.2px}
.table-card .desc{font-size:11.5px;color:var(--t3);margin-top:2px;margin-bottom:10px}
.table-card h3 + .bar-list{margin-top:10px}
.bar-list{flex:1}
.bar-item{
  display:flex;align-items:center;gap:9px;
  padding:6px 6px;border-radius:9px;font-size:12.5px;transition:background .15s;
}
.bar-item:hover{background:var(--hover)}
.bar-item:hover .bar-fill{filter:saturate(1.15) brightness(1.06)}
.bar-item.sub{padding-left:28px;font-size:12px;opacity:.92}
.rank{
  flex:none;width:18px;height:18px;border-radius:6px;display:grid;place-items:center;
  font-size:10.5px;font-weight:700;color:var(--t3);background:var(--track);
  font-variant-numeric:tabular-nums;transition:transform .18s cubic-bezier(.2,.8,.2,1);
}
.bar-item:hover .rank{transform:scale(1.12)}
.rank.r1{background:linear-gradient(135deg,#fbbf24,#f59e0b)!important;color:#fff!important;box-shadow:0 3px 8px -3px rgba(245,158,11,.6)}
.rank.r2{background:linear-gradient(135deg,#cbd5e1,#94a3b8)!important;color:#fff!important;box-shadow:0 3px 8px -3px rgba(148,163,184,.6)}
.rank.r3{background:linear-gradient(135deg,#fdba74,#fb923c)!important;color:#fff!important;box-shadow:0 3px 8px -3px rgba(251,146,60,.6)}

/* 名称列固定宽度对齐 */
.bar-label{
  flex:none;
  width:92px;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
  font-weight:550;font-variant-numeric:tabular-nums;
}
.tc-hosts .bar-label{width:120px;} /* 主机名较长，稍微放宽 */

.bar-label.clickable{cursor:pointer;color:var(--brand)}
.bar-label.clickable:hover{text-decoration:underline}
.bar-track{flex:1 1 28px;height:7px;border-radius:99px;background:var(--track);overflow:hidden}
.bar-fill{
  height:100%;border-radius:99px;
  background:linear-gradient(90deg,#3b82f6,#60a5fa);
  transition:width .55s cubic-bezier(.2,.8,.2,1),filter .18s;
}
.bar-fill.sub{background:linear-gradient(90deg,#93c5fd,#bfdbfe)}
.bar-nums{flex:none;display:flex;align-items:baseline;justify-content:flex-end;gap:6px;min-width:78px}
.bar-num{font-weight:650;font-variant-numeric:tabular-nums}
.bar-pct{font-size:10.5px;color:var(--t3);font-variant-numeric:tabular-nums}

/* === 多色榜单主题 === */
/* Hosts (蓝) */
.tc-hosts .bar-fill{background:linear-gradient(90deg,#3b82f6,#60a5fa)}
.tc-hosts .bar-fill.sub{background:linear-gradient(90deg,#93c5fd,#bfdbfe)}
.tc-hosts .rank{color:#3b82f6;background:rgba(59,130,246,.12)}
/* IPs (绿) */
.tc-ips .bar-fill{background:linear-gradient(90deg,#10b981,#34d399)}
.tc-ips .bar-fill.sub{background:linear-gradient(90deg,#6ee7b7,#a7f3d0)}
.tc-ips .rank{color:#10b981;background:rgba(16,185,129,.12)}
/* Regions (橙) */
.tc-regions .bar-fill{background:linear-gradient(90deg,#f59e0b,#fbbf24)}
.tc-regions .bar-fill.sub{background:linear-gradient(90deg,#fcd34d,#fde68a)}
.tc-regions .rank{color:#f59e0b;background:rgba(245,158,11,.12)}
.tc-regions .bar-label.clickable{color:#d97706}
/* Clients (紫) */
.tc-clients .bar-fill{background:linear-gradient(90deg,#8b5cf6,#a78bfa)}
.tc-clients .bar-fill.sub{background:linear-gradient(90deg,#c4b5fd,#ddd6fe)}
.tc-clients .rank{color:#8b5cf6;background:rgba(139,92,246,.12)}

/* 暗色模式兼容 */
[data-theme="dark"] .bar-fill.sub{background:linear-gradient(90deg,#1d4ed8,#3b82f6)}
[data-theme="dark"] .tc-hosts .bar-fill.sub{background:linear-gradient(90deg,#1d4ed8,#3b82f6)}
[data-theme="dark"] .tc-ips .bar-fill.sub{background:linear-gradient(90deg,#047857,#10b981)}
[data-theme="dark"] .tc-regions .bar-fill.sub{background:linear-gradient(90deg,#b45309,#f59e0b)}
[data-theme="dark"] .tc-clients .bar-fill.sub{background:linear-gradient(90deg,#5b21b6,#8b5cf6)}

.pager{
  display:flex;align-items:center;justify-content:center;gap:8px;
  margin-top:auto;padding-top:12px;border-top:1px solid var(--bd);
  font-size:11px;color:var(--t3);
}
.pager button{
  padding:4px 10px;border-radius:7px;font-size:11px;font-family:inherit;cursor:pointer;
  border:1px solid var(--bd2);background:var(--card-solid);color:var(--t2);transition:.15s;
}
.pager button:hover:not(:disabled){border-color:#93c5fd;color:var(--brand);transform:translateY(-1px)}
.pager button:active:not(:disabled){transform:translateY(0)}
.pager button:disabled{opacity:.35;cursor:not-allowed}

.empty{
  display:flex;flex-direction:column;align-items:center;justify-content:center;gap:9px;
  color:var(--t3);font-size:12px;text-align:center;
  padding:24px 12px;margin:8px 0;
  border:1px dashed var(--bd2);border-radius:12px;
}
.empty::before{
  content:"";width:34px;height:34px;opacity:.8;
  background:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%2394a3b8' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M3 3v18h18'/%3E%3Cpath d='M7 14l4-4 3 3 5-6'/%3E%3C/svg%3E") center/contain no-repeat;
}

/* ---------- 骨架屏 ---------- */
.sk-cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:16px}
.sk-card,.sk-chart{
  border-radius:var(--radius);border:1px solid var(--bd);
  background:linear-gradient(90deg,var(--track) 25%,rgba(148,163,184,.16) 37%,var(--track) 63%);
  background-size:800px 100%;
  animation:shimmer 1.4s infinite linear;
}
.sk-card{height:112px}
.sk-chart{height:238px;margin-bottom:16px}
@keyframes shimmer{0%{background-position:-400px 0}100%{background-position:400px 0}}

/* ---------- 细节打磨 ---------- */
/* 内容加载后的浮现动效（display:none → block 会自动重新触发） */
@keyframes fadeUp{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
#content{animation:fadeUp .38s cubic-bezier(.2,.8,.2,1) both}
#loading{animation:fadeUp .3s ease both}
/* 键盘导航焦点样式 */
:focus-visible{outline:2px solid rgba(59,130,246,.55);outline-offset:2px}
.tab:focus-visible,.node-btn:focus-visible,.icon-btn:focus-visible,.pager button:focus-visible{outline-offset:1px;border-radius:9px}
/* 日期输入 hover */
.date-filter input[type="date"]:hover{border-color:#bfdbfe}
/* 减少动效偏好 */
@media (prefers-reduced-motion:reduce){
  *,*::before,*::after{animation-duration:.001s!important;animation-iteration-count:1!important;transition-duration:.001s!important;scroll-behavior:auto!important}
}

/* ---------- 响应式 ---------- */
@media (max-width:1100px){
  .cards,.sk-cards{grid-template-columns:repeat(2,1fr)}
  .tables{grid-template-columns:repeat(2,1fr)}
}
@media (max-width:720px){
  .header{padding:10px 14px}
  .container{padding:14px 14px 32px}
  .tables{grid-template-columns:1fr}
  .card-value{font-size:22px}
  .chart-wrap{height:150px}
  .bar-label{width:80px}
  .tc-hosts .bar-label{width:100px}
}
@media (max-width:420px){
  .cards,.sk-cards{grid-template-columns:1fr}
}
</style>
</head>
<body>
<div class="header">
  <div class="brand">
    <div class="logo">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 14l4-4"/>
        <path d="M3.34 19a10 10 0 1 1 17.32 0"/>
      </svg>
    </div>
    <div class="brand-text">
      <h1>流量看板</h1>
    </div>
  </div>
  <div class="header-actions">
    <div class="node-dropdown" id="nodeDropdown">
      <button class="node-btn" id="nodeBtn">
        <span id="nodeLabel">全部节点</span>
        <svg class="chev" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
      </button>
      <div class="node-menu" id="nodeMenu">
        <div class="node-item active" data-value="">全部节点</div>
      </div>
    </div>
    <button class="icon-btn" id="themeBtn" title="切换主题" aria-label="切换主题">
      <svg class="ico-moon" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
      <svg class="ico-sun" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>
    </button>
  </div>
</div>

<div class="container">
  <div class="toolbar">
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
      <span class="sep">→</span>
      <input type="date" id="endDate">
      <button class="btn-primary" onclick="loadCustom()">查询</button>
    </div>
  </div>

  <div id="loading">
    <div class="sk-cards">
      <div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div>
    </div>
    <div class="sk-chart"></div>
    <div class="sk-chart"></div>
  </div>

  <div id="content" style="display:none">
    <div class="cards">
      <div class="card req">
        <div class="card-top">
          <span class="card-label">区间请求</span>
          <span class="card-ico"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h4l3 8 4-16 3 8h4"/></svg></span>
        </div>
        <div class="card-value" id="vRequests">0</div>
        <div class="card-sub">当前筛选条件下的请求数</div>
      </div>
      <div class="card traffic">
        <div class="card-top">
          <span class="card-label">区间流量</span>
          <span class="card-ico"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7 17l5-5 4 4 5-7"/><path d="M3 21h18"/></svg></span>
        </div>
        <div class="card-value" id="vTraffic">0 GB</div>
        <div class="card-sub" id="vTrafficSub">上行 + 下行</div>
      </div>
      <div class="card ip">
        <div class="card-top">
          <span class="card-label">独立 IP</span>
          <span class="card-ico"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3a15 15 0 0 1 0 18M12 3a15 15 0 0 0 0 18"/></svg></span>
        </div>
        <div class="card-value" id="vIPs">0</div>
        <div class="card-sub">去重后的访问来源</div>
      </div>
      <div class="card block">
        <div class="card-top">
          <span class="card-label">拦截次数</span>
          <span class="card-ico"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M5.6 5.6l12.8 12.8"/></svg></span>
        </div>
        <div class="card-value" id="vBlocked">0</div>
        <div class="card-sub">被策略拦截的请求</div>
      </div>
    </div>

    <div class="chart-card">
      <div class="card-hd"><h3>流量消耗趋势</h3><span class="desc">各时段网卡流量（GB）</span></div>
      <div class="chart-wrap"><canvas id="trafficChart"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="card-hd"><h3>请求量趋势</h3><span class="desc">各时段请求数 · 北京时间</span></div>
      <div class="chart-wrap"><canvas id="reqChart"></canvas></div>
    </div>

    <div class="tables">
      <div class="table-card tc-hosts">
        <h3>Top 主机</h3><div class="desc">按请求次数排序</div>
        <div class="bar-list" id="tbHosts"></div>
        <div class="pager" id="pagerHosts"></div>
      </div>
      <div class="table-card tc-ips">
        <h3>Top IP</h3><div class="desc">已做隐私化处理</div>
        <div class="bar-list" id="tbIPs"></div>
        <div class="pager" id="pagerIPs"></div>
      </div>
      <div class="table-card tc-regions">
        <h3>地区分布</h3><div class="desc">点击「中国」展开省份</div>
        <div class="bar-list" id="tbRegions"></div>
      </div>
      <div class="table-card tc-clients">
        <h3>播放器分布</h3><div class="desc">按客户端归类</div>
        <div class="bar-list" id="tbClients"></div>
        <div class="pager" id="pagerClients"></div>
      </div>
    </div>
  </div>
</div>

<script>
// ==================== 前端状态 ====================
let trafficChart, reqChart, lastData = null;
let currentRange = "today", currentNode = "", customStart = "", customEnd = "";
const PAGE_SIZE = 8;
const tableState = { hosts: { data: [], page: 0 }, ips: { data: [], page: 0 }, clients: { data: [], page: 0 } };
let regionExpanded = false, regionCountries = [], regionProvinces = [];

// 初始化日期选择器
document.getElementById("startDate").value = document.getElementById("endDate").value = new Date().toISOString().slice(0, 10);

// ==================== 主题切换 ====================
document.getElementById("themeBtn").addEventListener("click", () => {
  const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", next);
  try { localStorage.setItem("theme", next); } catch (e) {}
  if (lastData) render(lastData); // 图表配色跟随主题重绘
});

// ==================== 节点选择器 ====================
const nodeDropdown = document.getElementById("nodeDropdown");
const nodeBtn = document.getElementById("nodeBtn");
const nodeMenu = document.getElementById("nodeMenu");
nodeBtn.addEventListener("click", e => {
  e.stopPropagation();
  nodeMenu.classList.toggle("show");
  nodeDropdown.classList.toggle("open", nodeMenu.classList.contains("show"));
});
document.addEventListener("click", () => {
  nodeMenu.classList.remove("show");
  nodeDropdown.classList.remove("open");
});
nodeMenu.addEventListener("click", e => {
  const item = e.target.closest(".node-item");
  if (!item) return;
  currentNode = item.dataset.value || "";
  document.getElementById("nodeLabel").textContent = item.textContent;
  nodeMenu.querySelectorAll(".node-item").forEach(el => el.classList.remove("active"));
  item.classList.add("active");
  nodeMenu.classList.remove("show");
  nodeDropdown.classList.remove("open");
  loadData();
});

// ==================== 时间范围切换 ====================
document.getElementById("tabs").addEventListener("click", e => {
  if (e.target.classList.contains("tab")) {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    e.target.classList.add("active");
    currentRange = e.target.dataset.range;
    const df = document.getElementById("dateFilter");
    if (currentRange === "custom") df.classList.add("show");
    else { df.classList.remove("show"); customStart = customEnd = ""; loadData(); }
  }
});

/** 自定义日期查询 */
function loadCustom() {
  const s = document.getElementById("startDate").value, e = document.getElementById("endDate").value;
  if (!s || !e) return alert("请选择日期");
  if (s > e) return alert("开始日期不能晚于结束日期");
  customStart = s; customEnd = e; loadData();
}

/** 字节格式化 */
function formatBytes(b) {
  if (b >= 1e12) return (b / 1e12).toFixed(2) + " TB";
  if (b >= 1e9) return (b / 1e9).toFixed(2) + " GB";
  if (b >= 1e6) return (b / 1e6).toFixed(2) + " MB";
  return (b / 1e3).toFixed(1) + " KB";
}

// ==================== 表格渲染 ====================

/** 排名徽章 class */
function rankCls(i) { return i === 1 ? "r1" : i === 2 ? "r2" : i === 3 ? "r3" : ""; }

/** 渲染分页表格（主机 / IP / 播放器） */
function renderTable(key, listId, pagerId) {
  const state = tableState[key];
  const data = state.data || [];
  const page = state.page;
  const slice = data.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const list = document.getElementById(listId);
  const pager = document.getElementById(pagerId);
  const max = data.length ? Math.max(...data.map(x => x[1])) : 1;
  const total = data.reduce((s, x) => s + x[1], 0) || 1;

  if (!slice.length) {
    list.innerHTML = '<div class="empty">暂无数据</div>';
    pager.innerHTML = '';
    return;
  }

  list.innerHTML = slice.map(([k, v], i) => {
    const idx = page * PAGE_SIZE + i + 1;
    const pct = (v / total * 100).toFixed(1);
    const width = Math.max(3, (v / max) * 100);
    return \`<div class="bar-item">
      <span class="rank \${rankCls(idx)}">\${idx}</span>
      <div class="bar-label" title="\${k}">\${k}</div>
      <div class="bar-track"><div class="bar-fill" style="width:\${width}%"></div></div>
      <div class="bar-nums"><span class="bar-num">\${v.toLocaleString()}</span><span class="bar-pct">\${pct}%</span></div>
    </div>\`;
  }).join("");

  const totalPages = Math.max(1, Math.ceil(data.length / PAGE_SIZE));
  pager.innerHTML = \`<button \${page <= 0 ? "disabled" : ""} onclick="changePage('\${key}', -1)">上一页</button><span>\${page + 1} / \${totalPages}</span><button \${page >= totalPages - 1 ? "disabled" : ""} onclick="changePage('\${key}', 1)">下一页</button>\`;
}

/** 翻页 */
function changePage(key, delta) {
  const state = tableState[key];
  const totalPages = Math.max(1, Math.ceil(state.data.length / PAGE_SIZE));
  state.page = Math.max(0, Math.min(totalPages - 1, state.page + delta));
  const map = { hosts: ["tbHosts", "pagerHosts"], ips: ["tbIPs", "pagerIPs"], clients: ["tbClients", "pagerClients"] };
  renderTable(key, map[key][0], map[key][1]);
}
window.changePage = changePage;

/** 渲染地区分布（中国可展开省份） */
function renderRegions() {
  const list = document.getElementById("tbRegions");
  const countries = regionCountries || [], provinces = regionProvinces || [];
  if (!countries.length) { list.innerHTML = '<div class="empty">暂无数据</div>'; return; }

  const total = countries.reduce((s, x) => s + x[1], 0) || 1;
  const maxC = Math.max(...countries.map(x => x[1]), 1);
  const maxP = provinces.length ? Math.max(...provinces.map(x => x[1]), 1) : 1;
  let html = "";

  countries.forEach(([name, v], i) => {
    const idx = i + 1;
    const pct = (v / total * 100).toFixed(1);
    const width = Math.max(3, (v / maxC) * 100);
    const canExpand = name === "中国" && provinces.length > 0;
    const arrow = canExpand ? (regionExpanded ? "▾ " : "▸ ") : "";
    const cls = canExpand ? "clickable" : "";
    const click = canExpand ? "toggleRegion()" : "";
    html += \`<div class="bar-item">
      <span class="rank \${rankCls(idx)}">\${idx}</span>
      <div class="bar-label \${cls}" onclick="\${click}">\${arrow}\${name}</div>
      <div class="bar-track"><div class="bar-fill" style="width:\${width}%"></div></div>
      <div class="bar-nums"><span class="bar-num">\${v.toLocaleString()}</span><span class="bar-pct">\${pct}%</span></div>
    </div>\`;

    if (canExpand && regionExpanded) {
      const pTotal = provinces.reduce((s, x) => s + x[1], 0) || 1;
      provinces.forEach(([pn, pv]) => {
        const pp = (pv / pTotal * 100).toFixed(1);
        const pw = Math.max(3, (pv / maxP) * 100);
        html += \`<div class="bar-item sub">
          <div class="bar-label">\${pn}</div>
          <div class="bar-track"><div class="bar-fill sub" style="width:\${pw}%"></div></div>
          <div class="bar-nums"><span class="bar-num">\${pv.toLocaleString()}</span><span class="bar-pct">\${pp}%</span></div>
        </div>\`;
      });
    }
  });
  list.innerHTML = html;
}

/** 切换省份展开 / 收起 */
function toggleRegion() { regionExpanded = !regionExpanded; renderRegions(); }
window.toggleRegion = toggleRegion;

// ==================== 图表工具 ====================

/** 主题相关配色 */
function chartTheme() {
  const dark = document.documentElement.getAttribute("data-theme") === "dark";
  return {
    grid: dark ? "rgba(148,163,184,.10)" : "#eef2f7",
    tick: dark ? "#64748b" : "#94a3b8",
    tipBg: dark ? "#1e293b" : "#0f172a",
    tipTitle: "#cbd5e1",
    tipBody: "#f8fafc",
  };
}

/** 折线图渐变填充 */
function lineGrad(canvas, rgb) {
  const ctx = canvas.getContext("2d");
  const h = (canvas.parentElement && canvas.parentElement.clientHeight) || 180;
  const g = ctx.createLinearGradient(0, 0, 0, h);
  g.addColorStop(0, "rgba(" + rgb + ",0.30)");
  g.addColorStop(1, "rgba(" + rgb + ",0)");
  return g;
}

/** 通用配置 */
function baseOptions(ct) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: ct.tipBg,
        titleColor: ct.tipTitle,
        bodyColor: ct.tipBody,
        padding: 12,
        cornerRadius: 12,
        displayColors: false,
        titleFont: { size: 11, weight: "600" },
        bodyFont: { size: 12 },
        titleMarginBottom: 6,
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        grid: { color: ct.grid, drawTicks: false },
        border: { display: false },
        ticks: { color: ct.tick, font: { size: 11 }, padding: 6, maxTicksLimit: 5 },
      },
      x: {
        grid: { display: false },
        border: { display: false },
        ticks: { color: ct.tick, font: { size: 11 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
      },
    },
  };
}

// ==================== 数据加载 ====================

/** 骨架屏 HTML（失败重试时恢复用） */
function skeletonHtml() {
  return '<div class="sk-cards">' + '<div class="sk-card"></div>'.repeat(4) + '</div>' +
         '<div class="sk-chart"></div><div class="sk-chart"></div>';
}

/** 加载节点列表到下拉菜单 */
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

/** 加载统计数据 */
async function loadData() {
  const loading = document.getElementById("loading");
  loading.innerHTML = skeletonHtml();
  loading.style.display = "block";
  document.getElementById("content").style.display = "none";
  try {
    const qs = new URLSearchParams({ range: currentRange, node: currentNode });
    if (customStart && customEnd) { qs.set("start", customStart); qs.set("end", customEnd); }
    const r = await fetch("/api/stats?" + qs + "&_=" + Date.now());
    render(await r.json());
  } catch (e) {
    loading.innerHTML = '<div class="empty">加载失败：' + e.message + '</div>';
  }
}

/** 渲染页面 */
function render(data) {
  lastData = data;
  const s = data.summary || {};
  document.getElementById("vRequests").textContent = (s.requests || 0).toLocaleString();
  document.getElementById("vTraffic").textContent = formatBytes(s.traffic_bytes || 0);
  document.getElementById("vTrafficSub").textContent = \`上行 \${formatBytes(s.up_bytes || 0)} · 下行 \${formatBytes(s.down_bytes || 0)}\`;
  document.getElementById("vIPs").textContent = (s.unique_ips || 0).toLocaleString();
  document.getElementById("vBlocked").textContent = (s.blocked || 0).toLocaleString();

  const series = data.series || [];
  const labels = series.map(x => x.label);
  const trafficData = series.map(x => ((x.traffic || 0) / 1e9).toFixed(2));
  const reqData = series.map(x => x.requests);

  const ct = chartTheme();

  // ---- 流量趋势 ----
  const tCanvas = document.getElementById("trafficChart");
  if (trafficChart) trafficChart.destroy();
  const tOpt = baseOptions(ct);
  tOpt.plugins.tooltip.callbacks = { label: c => "流量：" + Number(c.parsed.y).toFixed(2) + " GB" };
  trafficChart = new Chart(tCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [{
        data: trafficData,
        borderColor: "#10b981",
        backgroundColor: lineGrad(tCanvas, "16,185,129"),
        fill: true,
        tension: 0.38,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: "#10b981",
        pointHoverBorderColor: "#fff",
        pointHoverBorderWidth: 2,
        borderWidth: 2.4,
      }],
    },
    options: tOpt,
  });

  // ---- 请求趋势 ----
  const rCanvas = document.getElementById("reqChart");
  if (reqChart) reqChart.destroy();
  const rOpt = baseOptions(ct);
  rOpt.plugins.tooltip.callbacks = { label: c => "请求：" + Number(c.parsed.y).toLocaleString() };
  reqChart = new Chart(rCanvas, {
    type: "line",
    data: {
      labels,
      datasets: [{
        data: reqData,
        borderColor: "#3b82f6",
        backgroundColor: lineGrad(rCanvas, "59,130,246"),
        fill: true,
        tension: 0.38,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: "#3b82f6",
        pointHoverBorderColor: "#fff",
        pointHoverBorderWidth: 2,
        borderWidth: 2.4,
      }],
    },
    options: rOpt,
  });

  // ---- 榜单 ----
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

// 初始化
loadNodes();
loadData();
</script>
</body>
</html>`;
