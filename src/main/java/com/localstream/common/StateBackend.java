package com.localstream.common;

import java.util.Map;

/**
 * 状态存储的抽象接口，当前由 LocalDiskStateBackend 实现。
 */
public interface StateBackend {
    /**
     * 将一次 Checkpoint 的全量状态持久化。
     *
     * @param checkpointId 本次 Checkpoint 版本号
     * @param states       nodeId → 状态快照（value 已由节点序列化为 String）
     */
    void save(long checkpointId, Map<String, Map<String, String>> states) throws Exception;

    /**
     * 加载指定版本的全量状态。
     *
     * @return nodeId → 状态快照；若版本不存在或文件损坏，抛出 Exception
     */
    Map<String, Map<String, String>> load(long checkpointId) throws Exception;

    /**
     * 返回当前已持久化的最新 Checkpoint ID；若无任何记录则返回 -1。
     */
    long latestCheckpointId() throws Exception;

    /**
     * 返回指定 checkpointId 对应的存储路径字符串（供 CheckpointInfo 展示用）。
     */
    String pathOf(long checkpointId);
}
