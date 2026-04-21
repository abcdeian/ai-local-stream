package com.localstream.common;

/**
 * 打包 keyBy 算子所需的两个函数对象。
 * 由 DataSet.keyBy() 创建，存入 OperatorNode.function 字段，由 runtime 的 ProcessorTask 消费。
 */
public class KeyByConfig {
    public final KeySelector<?, ?> keySelector;
    public final AggregateFunction<?, ?, ?> aggregateFunction;

    public KeyByConfig(KeySelector<?, ?> keySelector,
                       AggregateFunction<?, ?, ?> aggregateFunction) {
        this.keySelector = keySelector;
        this.aggregateFunction = aggregateFunction;
    }
}
