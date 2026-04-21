package com.localstream.common;

import java.util.Map;

/**
 * Checkpoint ack 回调接口，由 CheckpointCoordinator 实现，注入到 ProcessorTask 和 SinkTask 中。
 * 算子完成本节点快照后通过此接口上报 Coordinator，避免 runtime 子域直接依赖 checkpoint 子域。
 */
public interface CheckpointAckListener {
    /**
     * 算子处理完 CheckpointBarrier 后回调，上报快照状态。
     *
     * @param checkpointId  本次 Checkpoint 版本号
     * @param nodeId        当前节点 ID
     * @param stateSnapshot 当前节点状态快照（无状态节点传空 Map）
     */
    void onBarrierProcessed(long checkpointId, String nodeId, Map<String, String> stateSnapshot);
}
