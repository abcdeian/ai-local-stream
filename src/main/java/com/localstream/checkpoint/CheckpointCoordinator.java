package com.localstream.checkpoint;

import com.localstream.common.CheckpointAckListener;
import com.localstream.common.CheckpointInfo;
import com.localstream.common.Checkpointable;
import com.localstream.common.StateBackend;
import com.localstream.runtime.SourceTask;
import com.localstream.util.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 实现 CheckpointAckListener 接口，协调基于 Barrier 的 Checkpoint 全流程：
 * 向所有 SourceTask 注入 CheckpointBarrier → 等待所有非 Source 节点 ack → 汇总状态持久化 → 更新 CheckpointInfo。
 * 同时负责启动时的状态恢复（restore）。
 * 任何步骤失败只记录日志，不中断主任务（对应约束 EC-002）。
 */
public class CheckpointCoordinator implements CheckpointAckListener {

    private static final Logger log = Logger.getLogger(CheckpointCoordinator.class);

    private final List<SourceTask> sourceTasks;
    private final int nonSourceNodeCount;
    private final Map<String, Checkpointable> checkpointableMap;
    private final StateBackend stateBackend;
    private final long ackTimeoutMs;
    private final CheckpointInfo checkpointInfo = new CheckpointInfo();
    /** 防止并发触发（同时只执行一次） */
    private final AtomicBoolean executing = new AtomicBoolean(false);
    /** 进行中的 Checkpoint 上下文 */
    private final ConcurrentHashMap<Long, CheckpointContext> pendingCheckpoints = new ConcurrentHashMap<>();

    public CheckpointCoordinator(List<SourceTask> sourceTasks,
                                 int nonSourceNodeCount,
                                 Map<String, Checkpointable> checkpointableMap,
                                 StateBackend stateBackend,
                                 long ackTimeoutMs) {
        this.sourceTasks = sourceTasks;
        this.nonSourceNodeCount = nonSourceNodeCount;
        this.checkpointableMap = checkpointableMap;
        this.stateBackend = stateBackend;
        this.ackTimeoutMs = ackTimeoutMs;
    }

    /**
     * 执行一次完整 Checkpoint。
     * 通过 executing CAS 保证同一时刻只有一次 Checkpoint 在执行。
     * 任何异常均捕获并记录 error 日志，不向外抛（对应约束 EC-002）。
     */
    public void executeCheckpoint(long checkpointId) {
        if (!executing.compareAndSet(false, true)) {
            log.warn("Previous checkpoint not finished, skipping id={}", checkpointId);
            return;
        }

        try {
            log.info("Starting checkpoint id={}", checkpointId);

            // 1. 创建本次 Checkpoint 的上下文
            CheckpointContext context = new CheckpointContext(checkpointId, nonSourceNodeCount);
            pendingCheckpoints.put(checkpointId, context);

            // 2. 向所有 Source 注入 Barrier（Barrier 随数据流自然向下游传播，Source 无需暂停）
            for (SourceTask source : sourceTasks) {
                source.injectBarrier(checkpointId);
            }

            // 如果没有非 Source 节点（极端情况），直接跳过等待
            if (nonSourceNodeCount == 0) {
                stateBackend.save(checkpointId, context.collectedStates);
                updateSuccess(checkpointId);
                return;
            }

            // 3. 等待所有非 Source 节点 ack（CountDownLatch 超时等待）
            if (!context.latch.await(ackTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new Exception("Checkpoint ack timeout, checkpointId=" + checkpointId
                        + " waited=" + ackTimeoutMs + "ms");
            }

            // 4. 全部 ack 到齐，持久化收集到的所有状态
            stateBackend.save(checkpointId, context.collectedStates);

            // 5. 更新 CheckpointInfo（成功）
            updateSuccess(checkpointId);
            log.info("Checkpoint {} completed successfully", checkpointId);

        } catch (Exception e) {
            log.error("Checkpoint {} failed: {}", checkpointId, e.getMessage());
            checkpointInfo.latestStatus = "FAILED";
            // 不抛异常，不中断任务
        } finally {
            pendingCheckpoints.remove(checkpointId);
            executing.set(false);
        }
    }

    /**
     * CheckpointAckListener 实现：由 ProcessorTask / SinkTask 在处理完 Barrier 后回调。
     * 线程安全：多个任务线程并发调用，ConcurrentHashMap + CountDownLatch 保证安全。
     */
    @Override
    public void onBarrierProcessed(long checkpointId, String nodeId,
                                   Map<String, String> stateSnapshot) {
        CheckpointContext context = pendingCheckpoints.get(checkpointId);
        if (context == null) return; // 该 Checkpoint 已超时或已完成，忽略过期 ack
        context.collectedStates.put(nodeId, stateSnapshot);
        context.latch.countDown();
        log.info("Checkpoint {} ack from node [{}], remaining={}",
                checkpointId, nodeId, context.latch.getCount());
    }

    /**
     * 启动时恢复指定版本的状态。
     * 加载失败（stateBackend 抛异常）向上传播，拒绝启动（对应约束 EC-003）。
     */
    public void restore(long checkpointId) throws Exception {
        log.info("Restoring from checkpointId={}", checkpointId);
        Map<String, Map<String, String>> states = stateBackend.load(checkpointId);
        for (Map.Entry<String, Checkpointable> entry : checkpointableMap.entrySet()) {
            String nodeId = entry.getKey();
            Map<String, String> stateData = states.get(nodeId);
            if (stateData != null) {
                entry.getValue().restoreState(stateData);
                log.info("Restored state for node [{}]", nodeId);
            }
        }
        log.info("Restore completed from checkpointId={}", checkpointId);
    }

    /** 返回当前 CheckpointInfo 供 web 子域查询（只读） */
    public CheckpointInfo getCheckpointInfo() {
        return checkpointInfo;
    }

    private void updateSuccess(long checkpointId) {
        checkpointInfo.totalCount++;
        checkpointInfo.latestCheckpointId = checkpointId;
        checkpointInfo.latestStatus = "SUCCESS";
        checkpointInfo.latestStoragePath = stateBackend.pathOf(checkpointId);
        checkpointInfo.latestTimestampMs = System.currentTimeMillis();
    }

    /** 内部类：一次 Checkpoint 的上下文 */
    private static class CheckpointContext {
        final long checkpointId;
        final ConcurrentHashMap<String, Map<String, String>> collectedStates = new ConcurrentHashMap<>();
        final CountDownLatch latch;

        CheckpointContext(long checkpointId, int expectedAcks) {
            this.checkpointId = checkpointId;
            this.latch = new CountDownLatch(expectedAcks);
        }
    }
}
