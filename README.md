# Emby Proxy 流量监控看板

基于 Cloudflare Workers + KV 的轻量流量监控前端，接收 Emby Proxy 后台上报数据，提供可视化看板。

## 功能

- 接收上报并按「节点 + 日期」聚合存储
- 相同 `batch_id` 自动去重
- 今天 / 昨天 / 近 7 天结果缓存，只有新数据才更新
- 支持节点筛选、快捷时间范围、自定义日期
- 主机与 IP 脱敏显示
- 单天完整 24 小时图表（缺失小时补 0）
- 悬停显示具体数值
- 手机端标签栏可横向滚动

## 部署

1. 创建 Cloudflare Worker
2. 绑定 KV 命名空间，变量名 `REPORTS`
3. （可选）设置环境变量 `REPORT_TOKEN` 作为上报鉴权
4. 将 `worker.js` 内容粘贴到 Worker 并部署

## 上报接口

`POST /collect`

请求头（可选）：

```
Authorization: Bearer <REPORT_TOKEN>
```

请求体示例：

```json
{
  "batch_id": "nodekey-2026-11-24-10",
  "period_date": "2026-11-24",
  "period_hour": "10",
  "node_key": "xxx",
  "note": "美西",
  "requests": 38,
  "up_bytes": 655869,
  "down_bytes": 943761,
  "blocked": 2,
  "hosts": { "example.com": 34 },
  "ips": { "1.2.3.4": 34 },
  "countries": { "中国": 38 },
  "provinces": { "广东": 20 }
}
```

## 页面

访问 Worker 根路径即可打开看板。

## 存储说明

| Key 前缀            | 用途         | 过期  |
| ------------------- | ------------ | ----- |
| `agg:{node}:{date}` | 每日聚合数据 | 90 天 |
| `seen:{batch_id}`   | 去重标记     | 7 天  |
| `nodes:index`       | 节点列表     | 永久  |
| `ready:{range}:all` | 预计算缓存   | 2 天  |

免费额度足够长期使用。
