package com.localstream.common;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * keyby 算子的聚合逻辑。K 为分组键类型，T 为输入数据类型，R 为输出结果类型。
 * 继承 Checkpointable 接口，override snapshotState()/restoreState() 可保护状态。
 */
public interface AggregateFunction<K, T, R> extends Checkpointable {
    /**
     * 将 key 和一条数据累加到内部状态，返回本次产生的结果列表（空列表表示无输出）
     */
    List<R> add(K key, T value);

    /** 默认空实现：不保存状态 */
    @Override
    default Map<String, String> snapshotState() {
        return Collections.emptyMap();
    }

    /** 默认空实现：不恢复状态 */
    @Override
    default void restoreState(Map<String, String> state) {
    }
}
