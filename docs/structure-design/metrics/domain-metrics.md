# L3 — metrics 领域

## 职责
维护各 Source 节点的读取计数、各 Sink 节点的写出计数，以及对应的实时吞吐率（RPS），对外提供原子累加和快照查询能力。metrics 领域不感知数据内容，只处理计数与速率。

## 核心流程

### 计数上报
1. runtime 子域的 SourceTask 每成功 fetch 一条数据后，调用 metrics 领域上报一次读取事件（nodeId）
2. runtime 子域的 SinkTask 每成功 invoke 一次后，调用 metrics 领域上报一次写出事件（nodeId）
3. metrics 领域对每个 nodeId 维护一个 `AtomicLong` 总计数器，原子递增
4. metrics 领域同时维护一个 60 秒滑动窗口，在窗口内累计增量，用于计算 RPS；窗口到期后重置并输出本周期平均 RPS

### 快照查询
1. 调用方（web 子域）请求快照
2. metrics 领域遍历所有节点，为每个节点生成两条 `MetricEntry`：
   - metricName = `"read_count"` / `"write_count"`，metricValue = 总计数
   - metricName = `"read_rps"` / `"write_rps"`，metricValue = 当前每秒记录数（整数）
3. 封装为 `MetricsSnapshot`（含 entries 列表和采集时间戳）返回

## 组件划分

| 组件 | 职责 |
|------|------|
| MetricsRegistry | 持有 nodeId → AtomicLong 总计数器和 RPS 滑动窗口；提供 increment(nodeId) 方法供 runtime 上报；提供 snapshot() 方法产出 MetricsSnapshot |

## 关键约束
- 计数器使用 `AtomicLong`，保证多线程并发上报时数据正确（对应约束 CC-005）
- 仅统计 SOURCE 和 SINK 节点，中间算子不注册计数器（对应约束 FC-005）
- nodeName 在初始化时由 runtime 一并注册，metrics 领域不自行解析 JobGraph
- RPS 为近似值（基于最近一个统计窗口），仅用于展示，不作为精确指标
