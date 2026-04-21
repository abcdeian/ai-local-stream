# L4 — CP-008 SinkTask

## 组件概述

所属领域：runtime

职责：封装 SINK 节点的运行循环。持续从上游 `DataQueue` 消费数据，调用 `SinkFunction.invoke()` 写出，并通知 `MetricsRegistry` 计数写出条数。
作为 pipeline 末端节点，收到 `CheckpointBarrier` 时执行本节点状态快照并向 Coordinator ack，**不转发 Barrier**（无下游）。

## 内部结构

```
SinkTask
 ├── OperatorNode node               // 含 SinkFunction
 ├── DataQueue upstreamQueue         // 上游数据队列
 ├── MetricsRegistry metricsRegistry
 ├── CheckpointAckListener ackListener  // Barrier 处理完成后的回调（可为 null）
 └── volatile boolean running
```

## 功能小块

### SinkTask

```java
public class SinkTask implements Runnable {

    public SinkTask(OperatorNode node, DataQueue upstreamQueue,
                    MetricsRegistry metricsRegistry);

    /**
     * 主循环：持续 take → 判断类型 → invoke/处理Barrier → 计数，直到 running=false 或线程被中断。
     * 任何未捕获异常向上抛出（fail-fast）。
     */
    @Override
    public void run();

    /** 停止运行循环（由 JobExecutor.stop() 调用）。 */
    public void stop();

    /** 返回节点 ID（供 JobExecutor 线程命名使用）。 */
    public String getNodeId();

    /**
     * 注入 CheckpointAckListener（由 StreamEnv 通过 TaskPlan.setAckListener() 注入，在 startThreads() 前完成）。
     * 未注入时 Barrier 会被静默忽略（Checkpoint 未启用时的降级行为）。
     */
    public void setAckListener(CheckpointAckListener ackListener);
}
```

## 核心流程（伪代码）

```
SinkTask.run():
  SinkFunction<T> sink = (SinkFunction<T>) node.function
  while running and not interrupted:
    Object record = upstreamQueue.take()    // 阻塞等待数据

    if record instanceof CheckpointBarrier:
      barrier = (CheckpointBarrier) record
      // SinkTask 是 pipeline 末端，无需转发 Barrier；SinkTask 无状态，传空 Map
      if ackListener != null:
        ackListener.onBarrierProcessed(barrier.checkpointId, node.nodeId, emptyMap)
    else:
      sink.invoke((T) record)
      metricsRegistry.increment(node.nodeId)
```

## 依赖约束

- 依赖同域组件：`DataQueue`
- 依赖 metrics 子域：`MetricsRegistry`
- 依赖 L2 定义：`OperatorNode`、`SinkFunction`、`CheckpointBarrier`、`CheckpointAckListener`
- 不依赖 checkpoint 子域实现（通过 `CheckpointAckListener` 接口解耦）
- 不依赖 web
