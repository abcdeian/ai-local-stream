package com.localstream.common;

import java.util.Collections;
import java.util.Map;

/**
 * 状态快照接口，由 checkpoint 子域统一调用。
 * 默认实现为空（无状态），需要保护状态时 override 两个方法即可。
 */
public interface Checkpointable {
    /**
     * 获取当前状态快照，由系统在 Checkpoint 时调用。
     * 默认实现：返回空 Map（无状态）。
     */
    default Map<String, String> snapshotState() {
        return Collections.emptyMap();
    }

    /**
     * 系统在任务恢复时调用，将历史快照注入实现方，由实现方恢复内部状态。
     * 默认实现：不做任何操作。
     */
    default void restoreState(Map<String, String> state) {
    }
}
