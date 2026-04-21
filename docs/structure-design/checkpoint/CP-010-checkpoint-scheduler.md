# L4 — CP-010 CheckpointScheduler

## 组件概述

所属领域：checkpoint

职责：持有定时器，按 `StreamConfig.checkpointIntervalMs` 周期触发 Checkpoint 执行；维护单调递增的 `checkpointId`（从 1 开始）；将触发信号委托给 `CheckpointCoordinator` 执行。

## 内部结构

```
CheckpointScheduler
 ├── ScheduledExecutorService scheduler    // 单线程定时调度器
 ├── CheckpointCoordinator coordinator
 ├── long intervalMs
 └── AtomicLong checkpointIdSeq            // 单调递增 ID 生成器，从 1 开始
```

## 功能小块

### CheckpointScheduler

```java
public class CheckpointScheduler {

    public CheckpointScheduler(long intervalMs, CheckpointCoordinator coordinator);

    /**
     * 启动定时调度，按 intervalMs 周期触发 Checkpoint。
     * 使用 scheduleAtFixedRate，第一次触发在 intervalMs 之后。
     * 每次触发：checkpointId = checkpointIdSeq.incrementAndGet()，
     * 然后调用 coordinator.executeCheckpoint(checkpointId)。
     * 若上一次 Checkpoint 尚未完成，本次触发直接跳过（coordinator 内部加锁保证）。
     */
    public void start();

    /**
     * 停止定时调度，关闭 ScheduledExecutorService。
     * 由 JobExecutor.shutdown() 调用。
     */
    public void stop();
}
```

## 核心流程（伪代码）

```
CheckpointScheduler.start():
  scheduler = Executors.newSingleThreadScheduledExecutor(
      thread -> { thread.setName("checkpoint-scheduler"); thread.setDaemon(true) })
  scheduler.scheduleAtFixedRate(
    task = {
      long id = checkpointIdSeq.incrementAndGet()
      coordinator.executeCheckpoint(id)     // 内部处理所有异常，不向外抛
    },
    initialDelay = intervalMs,
    period = intervalMs,
    unit = MILLISECONDS
  )

CheckpointScheduler.stop():
  scheduler.shutdownNow()
```

## 依赖约束

- 依赖同域组件：`CheckpointCoordinator`
- 仅依赖 JDK `java.util.concurrent.ScheduledExecutorService`
- `checkpointIdSeq` 使用 `AtomicLong` 保证线程安全（调度线程唯一，此处仅作防御）
