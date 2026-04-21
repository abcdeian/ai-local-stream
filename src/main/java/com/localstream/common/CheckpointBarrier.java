package com.localstream.common;

/**
 * 系统内部流控记录，与普通数据记录共用同一 DataQueue，承载 Checkpoint ID 随数据流传播。
 * 算子收到 Barrier 时触发本节点状态快照，无状态算子直接转发。
 * 用户代码不可见，不可创建。
 */
public final class CheckpointBarrier {
    /** Checkpoint 版本号，由 CheckpointCoordinator 分配 */
    final long checkpointId;

    public CheckpointBarrier(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public String toString() {
        return "CheckpointBarrier{id=" + checkpointId + "}";
    }
}
