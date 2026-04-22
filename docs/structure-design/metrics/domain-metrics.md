# L3 — metrics 领域

## 职责
维护 DAG 中**所有节点**的输入/输出计数及实时吞吐率（RPS），并保留每个节点最近 10 分钟的 RPS 历史序列，对外提供原子累加和快照查询能力。metrics 领域不感知数据内容，只处理计数与速率。

## 核心流程

### 节点注册
1. `StreamEnv` 在启动任务线程前，遍历 `JobGraph` 中所有节点，调用 metrics 领域逐一注册
2. 每个节点注册时分配：输入计数器、输出计数器、RPS 历史环形缓冲区（容量 120，每 5 秒一个采样点）
3. SOURCE 节点仅注册输出计数器（输入恒为 0）；SINK 节点仅注册输入计数器（输出恒为 0）；其余节点均注册输入和输出计数器

### 计数上报
1. runtime 子域各 Task 在处理数据时向 metrics 领域上报事件：
   - `SourceTask`：每成功 fetch 一条数据后，上报该节点的**输出事件**
   - `ProcessorTask`（FLATMAP / KEYBY / UNION）：每从上游队列取出一条数据时上报**输入事件**；每向下游队列放入一条数据时上报**输出事件**
   - `SinkTask`：每成功 invoke 一次后，上报该节点的**输入事件**
2. metrics 领域对每个 nodeId 的输入/输出分别维护 `AtomicLong` 计数器，原子递增

### RPS 历史采样
1. metrics 领域内部启动一个后台采样线程，每 5 秒触发一次采样
2. 每次采样时，遍历所有节点，计算当前 5 秒窗口内的增量 RPS，写入该节点的环形缓冲区（最多保留 120 个点，超出则覆盖最旧的点）
3. RPS 为整数近似值（窗口内增量 / 5），仅用于趋势展示

### 快照查询
1. 调用方（web 子域）请求快照
2. metrics 领域遍历所有节点，按拓扑顺序（`topologyOrder`）为每个节点生成一条 `NodeMetricsSummary`，包含：
   - `inputCount`：总输入计数（SOURCE 节点固定为 0）
   - `outputCount`：总输出计数（SINK 节点固定为 0）
   - `rpsHistory`：最近 10 分钟的 RPS 历史列表（最多 120 条 `RpsDataPoint`，按时间升序）
3. 封装为 `MetricsSnapshot`（含 `nodeMetrics` 列表和采集时间戳）返回

## 组件划分

| 组件 | 职责 |
|------|------|
| MetricsRegistry | 持有所有节点的输入/输出 `AtomicLong` 计数器和 RPS 环形缓冲区；提供 `incrementInput(nodeId)` / `incrementOutput(nodeId)` 供 runtime 上报；内置后台采样线程每 5 秒写入历史点；提供 `snapshot()` 方法产出 `MetricsSnapshot` |

## 关键约束
- 计数器使用 `AtomicLong`，保证多线程并发上报时数据正确（对应约束 CC-005）
- 覆盖 DAG 中**所有**节点，不再限于 SOURCE 和 SINK
- nodeName、nodeType、topologyOrder 在初始化时由 `StreamEnv` 一并注册，metrics 领域不自行解析 `JobGraph`
- RPS 历史为近似值（基于 5 秒采样窗口），仅用于趋势展示，不作为精确指标
- 后台采样线程随 `StreamEnv` 停止而终止，不阻塞 JVM 退出
