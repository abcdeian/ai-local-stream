# L2 — 全局约束 | LocalStream

## 技术约束

| 编号 | 约束描述 |
|------|----------|
| TC-001 | 开发语言为 Java 8，不使用 Java 9+ 特性 |
| TC-002 | 构建工具为 Maven，项目以单模块 JAR 形式打包 |
| TC-003 | 不引入外部中间件（无 Kafka、无 Redis、无数据库），仅使用 JDK 标准库 |
| TC-004 | 系统为单机运行，不支持分布式部署 |
| TC-005 | 节点间数据传递使用有界阻塞队列，背压天然成立；队列容量可配置，默认 1024 |
| TC-006 | Checkpoint 状态当前持久化到本地磁盘，路径可配置 |
| TC-007 | 所有用户自定义函数（SourceFunction、SinkFunction、FlatMapFunction 等）不得由系统内部实例化，必须由用户构造后传入 |

## 功能约束

| 编号 | 约束描述 |
|------|----------|
| FC-001 | DAG 中不允许存在有向环路；系统在启动时进行拓扑排序检测，发现环路则拒绝启动 |
| FC-002 | 同一个 DataSet 实例可被多个下游算子引用（一写多读扇出），但不允许同一个 DataSet 被 sink 多次后再往下流（不支持循环回边） |
| FC-003 | keyby 算子是系统中唯一的有状态算子，其聚合状态必须纳入 Checkpoint 保护 |
| FC-004 | 恢复时必须指定一个合法的 Checkpoint 版本号（或显式选择"最新版本"），不支持无版本恢复 |
| FC-005 | metrics 只统计 Source 节点的读取条数和 Sink 节点的写出条数，中间算子不统计 |
| FC-006 | 任务一旦启动，只能通过 JVM 进程退出来停止；当前不支持运行时动态停止单个算子 |

## 并发约束

| 编号 | 约束描述 |
|------|----------|
| CC-001 | 每个 Source 节点独占一个线程持续读取数据 |
| CC-002 | 每个非 Source/Sink 算子节点独占一个线程处理数据 |
| CC-003 | 每个 Sink 节点独占一个线程写出数据 |
| CC-004 | Checkpoint 触发线程与数据处理线程并发运行，不互相阻塞 |
| CC-005 | metrics 计数器使用原子类型，保证并发安全 |

## 错误处理约束

| 编号 | 约束描述 |
|------|----------|
| EC-001 | 数据处理链路中任意节点抛出未捕获异常，系统记录错误日志后立即终止整个任务 |
| EC-002 | Checkpoint 执行失败，系统记录错误日志，不影响数据处理任务继续运行 |
| EC-003 | 恢复时 Checkpoint 文件损坏或版本不存在，系统抛出明确异常并拒绝启动 |

## 设计约束

> 设计约束约束的是**实现者**：规定系统内部组件必须遵守的设计规则，以保证正确性和一致性。当前设计约束集中于 Checkpoint 机制的实现规范。

| 编号 | 约束描述 |
|------|----------|
| DC-001 | Checkpoint 必须采用 **Barrier 驱动机制**：触发时向所有 Source 的下游队列注入 `CheckpointBarrier`，Barrier 随数据流向下游传播；各算子收到 Barrier 后立即对本节点做状态快照，**Source 无需暂停，数据流不中断** |
| DC-002 | `CheckpointBarrier` 必须与普通数据记录共用同一 `DataQueue`，传播顺序严格一致；算子收到 Barrier 时，其之前的所有数据均已被本节点处理完毕，快照时机精确，不存在 in-flight 数据污染 |
| DC-003 | 分叉节点（fan-out）必须将 `CheckpointBarrier` 广播写入**所有**下游队列；每条下游分支独立接收 Barrier、执行快照并向 Coordinator ack，任何一条分支缺失均视为本次 Checkpoint 未完成 |
| DC-004 | 合流节点（UNION / fan-in）必须对 Barrier 执行**对齐（Alignment）**：收到第一个上游 Barrier 后阻塞该上游（停止消费）并继续消费其他上游，待**所有上游的 Barrier 均到达**后统一执行快照，再向下游转发一个 Barrier |
| DC-005 | 所有非 Source 节点（ProcessorTask、SinkTask）处理完 Barrier 后必须向 `CheckpointCoordinator` 发送 ack，附带本节点的状态快照；Coordinator **收到全部节点 ack 后**才将汇总状态持久化到 `StateBackend` |
