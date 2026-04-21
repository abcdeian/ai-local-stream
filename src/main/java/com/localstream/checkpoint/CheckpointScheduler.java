package com.localstream.checkpoint;

import com.localstream.util.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 持有定时器，按 checkpointIntervalMs 周期触发 Checkpoint 执行。
 * 维护单调递增的 checkpointId（从 1 开始）。
 */
public class CheckpointScheduler {

    private static final Logger log = Logger.getLogger(CheckpointScheduler.class);

    private final long intervalMs;
    private final CheckpointCoordinator coordinator;
    private final AtomicLong checkpointIdSeq = new AtomicLong(0);
    private volatile ScheduledExecutorService scheduler;

    public CheckpointScheduler(long intervalMs, CheckpointCoordinator coordinator) {
        this.intervalMs = intervalMs;
        this.coordinator = coordinator;
    }

    /**
     * 启动定时调度，按 intervalMs 周期触发 Checkpoint。
     * 第一次触发在 intervalMs 之后。
     */
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "checkpoint-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> {
            long id = checkpointIdSeq.incrementAndGet();
            try {
                coordinator.executeCheckpoint(id);
            } catch (Exception e) {
                log.error("Checkpoint scheduler error for id={}: {}", id, e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("CheckpointScheduler started, interval={}ms", intervalMs);
    }

    /**
     * 停止定时调度，关闭 ScheduledExecutorService。
     */
    public void stop() {
        ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdownNow();
            log.info("CheckpointScheduler stopped");
        }
    }
}
