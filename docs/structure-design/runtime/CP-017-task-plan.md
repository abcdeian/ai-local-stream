# L4 — CP-017 TaskPlan

## 组件概述

所属领域：runtime

职责：将 `JobGraph` 编译为可执行的任务网络（Task 实例 + DataQueue 连接）。
负责：
1. 按拓扑序为每个节点创建 SourceTask / ProcessorTask / SinkTask
2. 为相邻节点间创建 DataQueue，连接数据通道
3. 维护三类任务集合及 checkpointableMap（KEYBY 节点 nodeId → AggregateFunction）
4. 对外提供 checkpoint 所需数据（SourceTask 列表、Checkpointable 映射）及 AckListener 注入入口

不负责线程管理、任务状态追踪——这些由 `JobExecutor` 承担。

## 内部结构

```
TaskPlan
 ├── List<SourceTask> sourceTasks
 ├── List<ProcessorTask> processorTasks
 ├── List<SinkTask> sinkTasks
 └── Map<String, Checkpointable> checkpointableMap   // nodeId → Checkpointable（KEYBY 节点）
```

## 功能小块

### TaskPlan

```java
public class TaskPlan {

    /**
     * 按 JobGraph 拓扑序创建所有任务实例和节点间队列。
     * 任何初始化异常包装为 JobStartException 抛出。
     */
    public TaskPlan(JobGraph graph, StreamConfig config, MetricsRegistry metricsRegistry)
        throws JobStartException;

    /** 返回所有 SourceTask（供 CheckpointCoordinator 注入 Barrier 使用） */
    public List<SourceTask> getSourceTasks();

    /**
     * 返回非 Source 节点总数（ProcessorTask + SinkTask 数量之和）。
     * 供 CheckpointCoordinator 构造时传入，作为期望 ack 数量。
     */
    public int getNonSourceNodeCount();

    /** 返回 nodeId → Checkpointable 映射（供 CheckpointCoordinator restore 阶段使用） */
    public Map<String, Checkpointable> getCheckpointables();

    /**
     * 将 CheckpointAckListener 注入到所有 ProcessorTask 和 SinkTask。
     * 由 StreamEnv 在 TaskPlan 构造完成后、JobExecutor.startThreads() 之前调用。
     * Checkpoint 未启用时无需调用（任务会静默忽略 Barrier）。
     */
    public void setAckListener(CheckpointAckListener ackListener);

    /**
     * 返回所有任务（SourceTask + ProcessorTask + SinkTask）的有序合并列表。
     * 供 JobExecutor 遍历任务创建线程。
     */
    public List<? extends Runnable> getAllTasks();
}
```

## 核心流程（伪代码）

```
TaskPlan(graph, config, metricsRegistry):
  for node in graph.nodes (拓扑序):
    task = createTask(node, graph, config.queueCapacity, metricsRegistry)
    if task instanceof SourceTask:     sourceTasks.add(task)
    if task instanceof ProcessorTask:  processorTasks.add(task)
    if task instanceof SinkTask:       sinkTasks.add(task)
    // 仅 KEYBY 节点的 aggregateFunction 需要注册为 Checkpointable，以 node.nodeId 为 key
    if node.type == KEYBY:
      checkpointableMap.put(node.nodeId, ((KeyByConfig) node.function).aggregateFunction)
  // 注：DataQueue 在 createTask 内部按 JobGraph 边关系创建，每条边对应一个 DataQueue

TaskPlan.setAckListener(ackListener):
  for task in processorTasks: task.setAckListener(ackListener)
  for task in sinkTasks:       task.setAckListener(ackListener)

TaskPlan.getAllTasks():
  return sourceTasks + processorTasks + sinkTasks   // 拓扑序（Source 先，Sink 后）
```

## 依赖约束

- 依赖同域组件：`SourceTask`、`ProcessorTask`、`SinkTask`、`DataQueue`
- 依赖 metrics 子域：`MetricsRegistry`（传入 SourceTask/SinkTask，不自行管理）
- 依赖 L2 定义：`JobGraph`、`StreamConfig`、`Checkpointable`、`CheckpointAckListener`、`JobStartException`、`KeyByConfig`、`OperatorType`
- 不依赖 checkpoint 子域（通过 L2 的 `CheckpointAckListener` 接口解耦）
- 不依赖 `JobExecutor`
