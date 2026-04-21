# L4 — CP-001 StreamEnv

## 组件概述

所属领域：api

职责：用户编写流水线的唯一入口类，同时承担整体组件装配者（Assembler）的角色。持有 `StreamConfig` 和 DAG 构建上下文（`DagBuilder`），提供 `addSource()` 注册数据来源，提供 `start()` 触发校验、完成所有子域组件的创建与装配、按序启动任务并阻塞主线程直到结束。

## 内部结构

```
StreamEnv
 ├── StreamConfig config
 └── DagBuilder dagBuilder
```

## 功能小块

### StreamEnv

```java
public class StreamEnv {

    private final StreamConfig config;
    private final DagBuilder dagBuilder;

    /** 使用默认配置创建 */
    public StreamEnv();

    /** 使用自定义配置创建 */
    public StreamEnv(StreamConfig config);

    /**
     * 注册一个 Source 节点，返回代表该节点输出流的 DataSet。
     * 内部创建 OperatorNode(type=SOURCE, function=sourceFunction)，
     * 注册到 DagBuilder，返回包裹该节点的 DataSet。
     */
    public <T> DataSet<T> addSource(SourceFunction<T> sourceFunction);

    /**
     * 触发整个流水线的校验、装配与启动。流程见【核心流程】。
     * start() 成功后当前线程阻塞，直到任务结束（所有任务线程 join 完成）。
     */
    public void start();

    /** 获取当前配置（供 DataSet 内部使用） */
    StreamConfig getConfig();

    /** 获取 DagBuilder（供 DataSet 内部注册节点使用） */
    DagBuilder getDagBuilder();
}
```

## 核心流程（伪代码）

```
StreamEnv.start():
  // 1. DAG 构建与校验
  JobGraph graph = dagBuilder.build()
  new DagValidator().validate(graph)    // 失败 → 抛 DagValidationException（含环路/无Sink检查）

  // 1b. DAG 优化：移除所有无法到达任意 Sink 的死节点
  new DagOptimizer().optimize(graph)

  // 2. 创建 MetricsRegistry，注册 SOURCE/SINK 节点
  MetricsRegistry metricsRegistry = new MetricsRegistry()
  for node in graph.nodes where type == SOURCE or SINK:
    metricsRegistry.register(node)   // 传入完整 OperatorNode，Registry 内部取 nodeId/name/type

  // 3. 编译任务网络（按 JobGraph 拓扑序创建所有 Task 实例和 DataQueue，不启动线程）
  TaskPlan plan = new TaskPlan(graph, config, metricsRegistry)

  // 3b. 创建 JobExecutor（仅负责线程生命周期管理）
  JobExecutor executor = new JobExecutor(plan, config)

  // 4. Checkpoint 相关装配（如启用）
  CheckpointCoordinator coordinator = null
  CheckpointScheduler scheduler = null
  if config.checkpointEnabled:
    StateBackend stateBackend = new LocalDiskStateBackend(config.checkpointDir)
    coordinator = new CheckpointCoordinator(
        plan.getSourceTasks(),
        plan.getNonSourceNodeCount(),
        plan.getCheckpointables(),
        stateBackend,
        config.checkpointAckTimeoutMs)
    // 4a. 将 coordinator 作为 CheckpointAckListener 注入到所有 ProcessorTask/SinkTask
    plan.setAckListener(coordinator)
    // 4b. 若指定恢复版本，执行状态恢复（失败则抛异常，拒绝启动）
    long restoreId = stateBackend.latestCheckpointId()   // 或用户指定
    if restoreId != -1:
      coordinator.restore(restoreId)
    scheduler = new CheckpointScheduler(config.checkpointIntervalMs, coordinator)

  // 5. 启动 Web 服务（如启用）
  WebServer webServer = null
  if config.webConfig.enabled:
    webServer = new WebServer(config, executor, metricsRegistry, coordinator)
    webServer.start()

  // 6. 注册全局 shutdown 钩子（任意线程异常时调用）
  Runnable shutdown = () -> {
    executor.stop()
    if scheduler != null: scheduler.stop()
    if webServer != null: webServer.stop()
  }
  executor.setShutdownHook(shutdown)

  // 7. 启动所有任务线程
  executor.startThreads()

  // 8. 启动 Checkpoint 调度（线程就绪后再开始调度）
  if scheduler != null: scheduler.start()

  // 9. 阻塞直到所有任务线程结束
  executor.awaitTermination()

  // 10. 清理
  shutdown.run()
```

## 依赖约束

- 依赖 dag 子域：`DagBuilder`、`DagValidator`、`DagOptimizer`
- 依赖 runtime 子域：`TaskPlan`、`JobExecutor`
- 依赖 checkpoint 子域：`CheckpointCoordinator`、`CheckpointScheduler`
- 依赖 state 子域：`LocalDiskStateBackend`
- 依赖 metrics 子域：`MetricsRegistry`
- 依赖 web 子域：`WebServer`
- 依赖 L2 定义：`StreamConfig`、`OperatorNode`、`JobGraph`、`DagValidationException`、`JobStartException`
- `StreamEnv` 是整个系统唯一的装配点，所有跨子域依赖在此汇聚，各子域组件本身不互相直接依赖
