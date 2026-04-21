# L4 — CP-007 ProcessorTask

## 组件概述

所属领域：runtime

职责：封装 FLATMAP / KEYBY / UNION 节点的运行循环。持续从上游 `DataQueue` 消费数据，根据节点类型执行对应算子逻辑，将结果写入所有下游 `DataQueue`。
处理 `CheckpointBarrier`：收到 Barrier 后执行状态快照，通过 `CheckpointAckListener` 上报 Coordinator，再将 Barrier 转发到所有下游队列。
KEYBY 节点通过 `((KeyByConfig) node.function).aggregateFunction` 直接获取 `Checkpointable` 实例做快照；FLATMAP / UNION 节点无状态，传空 Map。

## 内部结构

```
ProcessorTask（implements Runnable）
 ├── OperatorNode node                       // 节点类型与关联函数
 ├── List<DataQueue> upstreamQueues          // 上游队列（UNION 有 2 个，其余有 1 个）
 ├── List<DataQueue> downstreamQueues        // 下游队列（分叉时多个，线性时 1 个）
 ├── CheckpointAckListener ackListener       // Barrier 处理完成后的回调（可为 null）
 ├── Map<DataQueue, Long> barrierAlignState  // UNION 专用：已收到 Barrier 的上游队列 → checkpointId
 └── volatile boolean running
```

## 功能小块

### ProcessorTask

```java
public class ProcessorTask implements Runnable {

    public ProcessorTask(OperatorNode node,
                         List<DataQueue> upstreamQueues,
                         List<DataQueue> downstreamQueues);

    /**
     * 主循环：持续从上游队列取数据 → 执行算子 → 写入下游队列。
     * 根据 node.type 分发到对应处理方法。
     * 任何未捕获异常向上抛出（fail-fast）。
     */
    @Override
    public void run();

    /** 停止运行循环 */
    public void stop();

    /** 返回节点 ID（供 JobExecutor 线程命名使用）。 */
    public String getNodeId();

    /**
     * 注入 CheckpointAckListener（由 StreamEnv 通过 TaskPlan.setAckListener() 注入，在 startThreads() 前完成）。
     * 未注入时 Barrier 仍会被透传，但不发送 ack（Checkpoint 未启用时的降级行为）。
     */
    public void setAckListener(CheckpointAckListener ackListener);

    // ——— 私有处理方法 ———

    /**
     * 线性节点（FLATMAP/KEYBY）通用 Barrier 处理：
     * 获取状态快照 → ack Coordinator → 转发 Barrier 到所有下游队列。
     * KEYBY 节点：快照委托给 aggregateFunction.snapshotState()；
     * FLATMAP 节点：直接传空 Map（无状态）。
     */
    private void handleBarrier(CheckpointBarrier barrier);

    /**
     * FLATMAP 处理：record = upstreamQueues[0].take()；若为 Barrier 则走 handleBarrier()；
     * 否则 flatMap() → 写所有下游队列。
     */
    private void processFlatMap();

    /**
     * KEYBY 处理：record = upstreamQueues[0].take()；若为 Barrier 则走 handleBarrier()；
     * 否则 add(key, record) → 写所有下游队列。
     */
    private void processKeyBy();

    /**
     * UNION 处理（轮询消费 + Barrier 对齐）：
     * 轮询所有未被阻塞的上游队列；收到 Barrier 时阻塞对应队列等待其他队列的 Barrier；
     * 所有上游 Barrier 都到齐后（对齐）执行快照并 ack；
     * 普通记录写所有下游队列。
     */
    private void processUnion();
}
```

## 核心流程（伪代码）

```
ProcessorTask.run():
  while running and not interrupted:
    switch node.type:
      case FLATMAP: processFlatMap()
      case KEYBY:   processKeyBy()
      case UNION:   processUnion()

// ——— 线性节点（FLATMAP / KEYBY）———

processFlatMap():
  record = upstreamQueues[0].take()
  if record instanceof CheckpointBarrier:
    handleBarrier((CheckpointBarrier) record)
    return
  results = ((FlatMapFunction) node.function).flatMap(record)
  for r in results:
    for q in downstreamQueues: q.put(r)   // 分叉时写入所有下游

processKeyBy():
  record = upstreamQueues[0].take()
  if record instanceof CheckpointBarrier:
    handleBarrier((CheckpointBarrier) record)
    return
  config = (KeyByConfig) node.function
  key = config.keySelector.getKey(record)
  results = config.aggregateFunction.add(key, record)
  for r in results:
    for q in downstreamQueues: q.put(r)

// 线性节点通用 Barrier 处理
handleBarrier(barrier):
  // nodeId 直接从持有的 node 实例获取，无需 getNodeId() 方法
  stateMap = (node.type == KEYBY)
             ? ((KeyByConfig) node.function).aggregateFunction.snapshotState()
             : emptyMap
  if ackListener != null:
    ackListener.onBarrierProcessed(barrier.checkpointId, node.nodeId, stateMap)
  for q in downstreamQueues: q.put(barrier)   // 转发到所有下游（分叉时广播）


// ——— 合流节点（UNION）：Barrier 对齐 ———

// 设计说明：
// UNION 节点有多个上游队列，同一个 checkpointId 的 Barrier 会分别从各上游到达。
// 必须等所有上游的 Barrier 都到达（对齐）后才能做快照，否则快照时仍有部分上游数据未处理。
// 对齐期间：已收到 Barrier 的上游队列暂停消费（停止 poll），
//           未收到 Barrier 的上游继续正常消费，其普通记录照常下发。

processUnion():
  for queue in upstreamQueues:
    if barrierAlignState.containsKey(queue): continue   // 该队列已对齐，暂停消费

    record = queue.poll(1ms)                            // 非阻塞 poll
    if record == null: continue

    if record instanceof CheckpointBarrier:
      barrier = (CheckpointBarrier) record
      barrierAlignState.put(queue, barrier.checkpointId)

      if barrierAlignState.size() == upstreamQueues.size():
        // 所有上游 Barrier 已对齐
        checkpointId = barrier.checkpointId
        // UNION 节点无状态，传空 Map
        if ackListener != null:
          ackListener.onBarrierProcessed(checkpointId, node.nodeId, emptyMap)
        for q in downstreamQueues: q.put(new CheckpointBarrier(checkpointId))
        barrierAlignState.clear()                       // 对齐完成，恢复所有队列消费
    else:
      for q in downstreamQueues: q.put(record)
```

## 分叉节点（fan-out）与合流节点（UNION）说明

### 分叉节点（fan-out）
同一个节点输出被多个下游节点消费时，`TaskPlan` 为每个下游各创建一个 `DataQueue`，
ProcessorTask 持有 `List<DataQueue> downstreamQueues`（多个元素）。
- **普通记录**：写入所有下游队列（广播语义，调用方保证数据不可变）
- **Barrier**：同样写入所有下游队列 —— 每条分叉都必须独立接收到 Barrier，各自完成快照和 ack

### 合流节点（UNION / fan-in）
多个上游 DataStream 合并为一个节点时，ProcessorTask 持有多个 `upstreamQueues`。
Barrier 对齐规则（如上伪代码）：
1. 从上游 i 收到 Barrier → 阻塞上游 i（停止消费），记录到 `barrierAlignState`
2. 继续从未阻塞的其他上游消费普通记录（避免阻塞整个 pipeline）
3. 所有上游 Barrier 均到达 → 快照 → ack Coordinator → 转发 Barrier → 清空对齐状态、恢复所有上游

## 依赖约束

- 依赖同域组件：`DataQueue`
- 依赖 L2 定义：`OperatorNode`、`OperatorType`、`FlatMapFunction`、`AggregateFunction`、`KeyByConfig`、`CheckpointBarrier`、`CheckpointAckListener`
- 不依赖 checkpoint 子域实现（通过 `CheckpointAckListener` 接口解耦）
- 不依赖 api 子域（`KeyByConfig` 已升级为 L2 公共定义）
- 不依赖 metrics / web
