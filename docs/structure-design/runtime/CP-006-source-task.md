# L4 — CP-006 SourceTask

## 组件概述

所属领域：runtime

职责：封装 SOURCE 节点的运行循环。持续调用 `SourceFunction.fetch()` 读取数据，将结果写入所有下游 `DataQueue`，并通知 `MetricsRegistry` 计数。
支持 Checkpoint Barrier 注入：`CheckpointCoordinator` 调用 `injectBarrier()` 后，SourceTask 在下一轮循环开始时将 `CheckpointBarrier` 写入所有下游队列，数据流不中断。

## 内部结构

```
SourceTask
 ├── OperatorNode node                      // 对应的 DAG 节点（含 SourceFunction）
 ├── List<DataQueue> downstreamQueues       // 下游数据队列（fanout 时为多个）
 ├── MetricsRegistry metricsRegistry
 ├── volatile CheckpointBarrier pendingBarrier  // 待注入的 Barrier（null 表示无待注入）
 └── volatile boolean running               // 运行开关（false 时退出循环）
```

## 功能小块

### SourceTask

```java
public class SourceTask implements Runnable {

    public SourceTask(OperatorNode node, List<DataQueue> downstreamQueues,
                      MetricsRegistry metricsRegistry);

    /**
     * 主循环：持续 fetch → 写下游队列 → 计数，直到 running=false 或线程被中断。
     * 每轮循环开始前检查 pendingBarrier，若非 null 则优先将 Barrier 写入所有下游队列。
     * fetch 返回 null 时短暂 sleep（1ms）再继续，不计数。
     * 任何未捕获异常向上抛出，由线程的 UncaughtExceptionHandler 接管（fail-fast）。
     */
    @Override
    public void run();

    /**
     * 由 CheckpointCoordinator 调用，触发 Barrier 注入。
     * 设置 pendingBarrier；run 循环在下一轮检测到后将其写入所有下游队列，不阻塞 Source。
     * 线程安全：volatile 写保证可见性。
     */
    public void injectBarrier(long checkpointId);

    /** 停止运行循环（由 JobExecutor.stop() 调用）。 */
    public void stop();

    /** 返回节点 ID（供 JobExecutor 线程命名使用）。 */
    public String getNodeId();
}
```

## 核心流程（伪代码）

```
SourceTask.run():
  SourceFunction<T> source = (SourceFunction<T>) node.function
  while running and not interrupted:

    // 优先处理待注入的 Barrier（夹在两条普通记录之间）
    if pendingBarrier != null:
      barrier = pendingBarrier
      pendingBarrier = null
      for q in downstreamQueues: q.put(barrier)
      continue

    T record = source.fetch()
    if record == null:
      Thread.sleep(1)
      continue

    for q in downstreamQueues: q.put(record)   // fanout：逐一写入所有下游
    metricsRegistry.increment(node.nodeId)      // 读取计数 +1
```

## 依赖约束

- 依赖同域组件：`DataQueue`
- 依赖 metrics 子域：`MetricsRegistry`（接口调用）
- 依赖 L2 定义：`OperatorNode`、`SourceFunction`、`CheckpointBarrier`
- 不依赖 checkpoint 子域（被动接受 injectBarrier 调用，不感知 Coordinator 实现）
