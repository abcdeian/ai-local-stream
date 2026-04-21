# L4 — CP-011 CheckpointCoordinator

## 组件概述

所属领域：checkpoint

职责：实现 `CheckpointAckListener` 接口，协调基于 Barrier 的 Checkpoint 全流程：
向所有 SourceTask 注入 `CheckpointBarrier` → 等待所有非 Source 节点 ack → 汇总状态持久化 → 更新 `CheckpointInfo`。
同时负责启动时的状态恢复（`restore`）。任何步骤失败只记录日志，不中断主任务（对应约束 EC-002）。

## 内部结构

```
CheckpointCoordinator
 ├── List<SourceTask> sourceTasks               // 用于注入 Barrier
 ├── int nonSourceNodeCount                     // 期望收到 ack 的节点数（= ProcessorTask + SinkTask 总数）
 ├── Map<String, Checkpointable> checkpointableMap   // nodeId → Checkpointable，用于 restore 阶段按 key 注入状态
 ├── StateBackend stateBackend
 ├── long ackTimeoutMs                          // ack 超时时间（来自 StreamConfig.checkpointAckTimeoutMs）
 ├── CheckpointInfo checkpointInfo              // 维护供 web 查询的状态摘要
 ├── AtomicBoolean executing                    // 防止并发触发（同时只执行一次）
 └── ConcurrentHashMap<Long, CheckpointContext> pendingCheckpoints  // 进行中的 Checkpoint 上下文
```

### 内部类 CheckpointContext

```
CheckpointContext
 ├── long checkpointId
 ├── ConcurrentHashMap<String, Map<String,String>> collectedStates  // nodeId → 状态快照
 └── CountDownLatch latch                       // 初始值 = nonSourceNodeCount，ack 到齐时归零
```

## 功能小块

### CheckpointCoordinator

```java
public class CheckpointCoordinator implements CheckpointAckListener {

    public CheckpointCoordinator(List<SourceTask> sourceTasks,
                                 int nonSourceNodeCount,
                                 Map<String, Checkpointable> checkpointableMap,
                                 StateBackend stateBackend,
                                 long ackTimeoutMs);

    /**
     * 执行一次完整 Checkpoint，流程见【核心流程】。
     * 由 CheckpointScheduler 定期调用。
     * 通过 executing CAS 保证同一时刻只有一次 Checkpoint 在执行；
     * 若上次未完成则本次跳过（记录 warn 日志）。
     * 任何异常均捕获并记录 error 日志，不向外抛（对应约束 EC-002）。
     */
    public void executeCheckpoint(long checkpointId);

    /**
     * CheckpointAckListener 实现：由 ProcessorTask / SinkTask 在处理完 Barrier 后回调。
     * 线程安全：多个任务线程并发调用，ConcurrentHashMap + CountDownLatch 保证安全。
     */
    @Override
    public void onBarrierProcessed(long checkpointId, String nodeId,
                                   Map<String, String> stateSnapshot);

    /**
     * 启动时恢复指定版本的状态。
     * 1. 调用 stateBackend.load(checkpointId) 加载全量状态
     * 2. 为每个 Checkpointable 节点按 nodeId 注入对应状态
     * 3. 加载失败（stateBackend 抛异常）向上传播，拒绝启动（对应约束 EC-003）
     */
    public void restore(long checkpointId) throws Exception;

    /** 返回当前 CheckpointInfo 供 web 子域查询（只读） */
    public CheckpointInfo getCheckpointInfo();
}
```

## 核心流程（伪代码）

```
CheckpointCoordinator.executeCheckpoint(checkpointId):
  if not executing.compareAndSet(false, true):
    log.warn("Previous checkpoint not finished, skipping id={}", checkpointId)
    return

  try:
    // 1. 创建本次 Checkpoint 的上下文，放入 pendingCheckpoints
    context = new CheckpointContext(checkpointId, nonSourceNodeCount)
    pendingCheckpoints.put(checkpointId, context)

    // 2. 向所有 Source 注入 Barrier
    //    Source 线程在下一轮循环取到 pendingBarrier 后，将 Barrier 写入其所有下游队列
    //    Barrier 随后随数据流自然向下游传播，无需 Source 暂停
    for source in sourceTasks: source.injectBarrier(checkpointId)

    // 3. 等待所有非 Source 节点 ack（CountDownLatch 超时等待）
    if not context.latch.await(ackTimeoutMs, MILLISECONDS):
      throw new Exception("Checkpoint ack timeout, checkpointId=" + checkpointId)

    // 4. 全部 ack 到齐，持久化收集到的所有状态
    stateBackend.save(checkpointId, context.collectedStates)

    // 5. 更新 CheckpointInfo（成功）
    checkpointInfo.totalCount++
    checkpointInfo.latestCheckpointId = checkpointId
    checkpointInfo.latestStatus = "SUCCESS"
    checkpointInfo.latestStoragePath = stateBackend.pathOf(checkpointId)
    checkpointInfo.latestTimestampMs = System.currentTimeMillis()

    log.info("Checkpoint {} completed successfully", checkpointId)

  catch Exception e:
    log.error("Checkpoint {} failed: {}", checkpointId, e.getMessage())
    checkpointInfo.latestStatus = "FAILED"
    // 不抛异常，不中断任务

  finally:
    pendingCheckpoints.remove(checkpointId)
    executing.set(false)


CheckpointCoordinator.onBarrierProcessed(checkpointId, nodeId, stateSnapshot):
  context = pendingCheckpoints.get(checkpointId)
  if context == null: return              // 该 Checkpoint 已超时或已完成，忽略过期 ack
  context.collectedStates.put(nodeId, stateSnapshot)
  context.latch.countDown()              // 到 0 时唤醒 executeCheckpoint 等待线程


CheckpointCoordinator.restore(checkpointId):
  // 1. 从 StateBackend 加载全量状态（失败时向上抛，拒绝启动）
  states = stateBackend.load(checkpointId)   // Map<nodeId, Map<String,String>>

  // 2. 按 nodeId 为各 Checkpointable 注入对应状态
  for (nodeId, checkpointable) in checkpointableMap:
    stateData = states.get(nodeId)
    if stateData != null:
      checkpointable.restoreState(stateData)

  log.info("Restore completed from checkpointId={}", checkpointId)
```

## 正确性说明

| 场景 | 处理方式 |
|------|----------|
| 线性节点（单上游）| Barrier 随数据流顺序到达，收到时已处理完所有前序记录，快照时机精确 |
| 分叉节点（fan-out）| Barrier 被广播到所有下游队列，每条分支独立 ack，Coordinator 收到所有 ack 才持久化 |
| 合流节点（UNION / fan-in）| ProcessorTask 做 Barrier 对齐：等所有上游 Barrier 到达后统一快照，避免部分上游数据未处理 |
| 数据流不中断 | Source 无需暂停，Barrier 夹在普通记录之间传播，吞吐不受影响 |

## 依赖约束

- 依赖 runtime 子域：`SourceTask`（injectBarrier）
- 依赖 state 子域：`StateBackend`（接口）
- 依赖 L2 定义：`Checkpointable`、`CheckpointInfo`、`CheckpointAckListener`、`CheckpointBarrier`
- 实现 `CheckpointAckListener` 接口，由 runtime 子域的 ProcessorTask/SinkTask 通过接口回调（无反向依赖）
