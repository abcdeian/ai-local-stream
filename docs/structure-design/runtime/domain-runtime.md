# L3 — runtime 领域

## 职责
将 `JobGraph` 转化为可执行的任务网络：`TaskPlan` 负责按拓扑序创建所有 Task 实例和 DataQueue；`JobExecutor` 负责为每个任务分配独立线程、管理生命周期（启动、运行、异常终止）；向 checkpoint 子域注册可检查点节点，向 metrics 子域上报读写事件，向 web 子域提供任务状态查询。

## 核心流程

1. `TaskPlan` 接收 `JobGraph`，按拓扑顺序为每个节点创建对应的任务包装对象（SourceTask / ProcessorTask / SinkTask），并为相邻节点间创建 DataQueue
2. 若配置了 checkpoint，将所有 KEYBY 节点的 `AggregateFunction`（即 `Checkpointable` 实现方）以 `node.nodeId` 为 key 注册到 checkpoint 子域
3. 为每个任务分配独立线程并启动；所有线程启动完成后任务进入运行状态
4. 各 SourceTask 持续调用 `SourceFunction.fetch()` 读取数据，写入下游队列，并通知 metrics 计数
5. 各 ProcessorTask 持续从上游队列读取数据，执行算子逻辑（FlatMap / KeyBy），将结果写入下游队列
6. 各 SinkTask 持续从上游队列读取数据，调用 `SinkFunction.invoke()`，并通知 metrics 计数
7. 任意线程抛出未捕获异常时，runtime 捕获后记录日志并调用全局终止，关闭所有线程

## 组件划分

| 组件 | 职责 |
|------|------|
| TaskPlan | 编译阶段；接收 JobGraph，按拓扑序创建所有 Task 实例和 DataQueue；维护任务集合及 checkpointableMap，提供 Checkpoint 相关查询接口 |
| JobExecutor | 执行阶段；接收 TaskPlan，为每个任务分配线程、管理启停，维护 TaskInfo 供外部查询 |
| SourceTask | 封装 SOURCE 节点逻辑；持续 fetch 数据，写入下游队列，上报读取计数 |
| ProcessorTask | 封装 FLATMAP / KEYBY / UNION 节点逻辑；从上游队列消费数据，执行算子，写入下游队列 |
| SinkTask | 封装 SINK 节点逻辑；从上游队列消费数据，调用 SinkFunction，上报写出计数 |
| DataQueue | 有界阻塞队列包装；连接相邻节点的数据通道；支持一写多读（fanout） |

## 关键约束
- 每个任务独占一个线程，线程数 = 节点数
- DataQueue 的写满阻塞提供天然背压，不需要额外流控机制
- UNION 节点的 ProcessorTask 持有两个上游队列，轮询消费
- runtime 不直接感知 StateBackend，Checkpoint 相关操作全部委托给 checkpoint 子域
