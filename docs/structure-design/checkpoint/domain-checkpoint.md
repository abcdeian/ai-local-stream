# L3 — checkpoint 领域

## 职责
周期性触发检查点流程，收集所有 `Checkpointable` 节点的状态快照，协调持久化；支持从指定版本恢复各节点状态。checkpoint 领域不感知底层存储细节，全部委托给 state 子域的 `StateBackend`。

## 核心流程

### 触发 Checkpoint（Barrier 驱动）
1. `CheckpointScheduler` 按 `StreamConfig.checkpointIntervalMs` 周期触发一次 Checkpoint
2. 生成单调递增的 `checkpointId`（从 1 开始）
3. 在 `pendingCheckpoints` 中创建本次 `CheckpointContext`（含 `CountDownLatch`，初始值 = 非 Source 节点总数）
4. 向所有 SourceTask 注入 `CheckpointBarrier(checkpointId)`；SourceTask 在下一轮循环将其写入所有下游队列，**Source 无需暂停，数据流不中断**（对应约束 DC-001）
5. Barrier 随数据流向下游传播：
   - **线性节点**：取出 Barrier → 快照 → ack Coordinator → 转发到下游
   - **分叉节点（fan-out）**：Barrier 广播写入所有下游队列（对应约束 DC-003）
   - **合流节点（UNION）**：Barrier 对齐，等所有上游 Barrier 到达后统一快照（对应约束 DC-004）
6. 每个非 Source 节点处理完 Barrier 后调用 `ackListener.onBarrierProcessed(checkpointId, nodeId, stateSnapshot)` 上报（对应约束 DC-005）
7. Coordinator 收到全部 ack → `CountDownLatch` 归零 → 将汇总状态交给 `StateBackend.save()` 持久化
8. 持久化成功后更新 `CheckpointInfo`
9. 任一步骤失败则记录错误日志，**不影响主任务继续运行**，等待下一周期重试（对应约束 EC-002）

### 恢复 Checkpoint
1. 启动时若指定恢复版本（或选择 latest），调用 `StateBackend.load(checkpointId)` 加载全量状态
2. 遍历已注册的 `Checkpointable` 节点，按 nodeId 找到对应状态，调用 `restoreState()` 注入
3. 全部节点恢复完成后通知 runtime 继续启动

## Checkpoint 正确性保障

Checkpoint 的正确性通过以下机制保证：

1. **写入原子性**：`StateBackend` 在写入所有节点状态文件完成后，最后写入 `_SUCCESS` 标记文件；若中途失败，`_SUCCESS` 不会被创建，该版本视为无效
2. **加载校验**：恢复时 `StateBackend.load()` 首先检查 `_SUCCESS` 标记是否存在；不存在则抛出异常，拒绝使用不完整的 checkpoint（对应约束 EC-003）
3. **版本隔离**：每个 checkpointId 对应独立目录，新版本写入不会覆盖旧版本，支持回退到任意历史版本
4. **快照一致性**：`CheckpointBarrier` 与普通记录共用同一队列，算子收到 Barrier 时其之前的所有数据均已处理完毕；UNION 节点等所有上游 Barrier 对齐后再快照，保证快照时无 in-flight 数据、所有算子状态精确一致（对应约束 DC-002、DC-004）

## 组件划分

| 组件 | 职责 |
|------|------|
| CheckpointScheduler | 持有定时器，按周期触发 checkpoint；维护单调递增的 checkpointId |
| CheckpointCoordinator | 协调一次完整 checkpoint（Barrier 驱动）：向所有 SourceTask 注入 Barrier，等待所有非 Source 节点 ack，汇总状态后调用 StateBackend 持久化，更新 CheckpointInfo 供 web 查询；失败时记录日志，不抛出异常 |

## 关键约束
- Checkpoint 采用 Barrier 驱动，Source 无需暂停，数据流不中断（对应约束 DC-001、DC-002）
- 分叉广播 Barrier，合流对齐 Barrier（对应约束 DC-003、DC-004）
- Coordinator 收到全部节点 ack 后才持久化（对应约束 DC-005）
- Checkpoint 失败只记录日志，不中断任务，等待下一周期重试（对应约束 EC-002）
- 恢复时 `StateBackend.load()` 抛出异常则拒绝启动（对应约束 EC-003）
