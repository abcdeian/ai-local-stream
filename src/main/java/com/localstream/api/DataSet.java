package com.localstream.api;

import com.localstream.common.AggregateFunction;
import com.localstream.common.FlatMapFunction;
import com.localstream.common.KeyByConfig;
import com.localstream.common.KeySelector;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.common.SinkFunction;

import java.util.Arrays;
import java.util.Collections;

/**
 * 代表 DAG 中某个节点输出的数据流句柄。
 * 用户通过链式调用在其上注册下游算子，每次调用都在 DagBuilder 中新增一个
 * OperatorNode 并返回代表新节点输出的 DataSet。
 * DataSet 本身不执行任何计算，只构建 DAG 结构。
 */
public class DataSet<T> {

    private final OperatorNode currentNode;
    private final StreamEnv env;

    /** 包私有构造，由 StreamEnv.addSource() 和各算子方法创建 */
    DataSet(OperatorNode currentNode, StreamEnv env) {
        this.currentNode = currentNode;
        this.env = env;
    }

    /**
     * 注册一个 FlatMap 算子节点。
     * 创建 OperatorNode(type=FLATMAP, function=function, upstream=[currentNode.nodeId])，
     * 注册到 DagBuilder，返回新节点的 DataSet&lt;R&gt;。
     */
    public <R> DataSet<R> flatMap(FlatMapFunction<T, R> function) {
        OperatorNode node = new OperatorNode(
                OperatorType.FLATMAP,
                function,
                Collections.singletonList(currentNode.nodeId));
        env.getDagBuilder().addNode(node);
        return new DataSet<>(node, env);
    }

    /**
     * 注册一个 KeyBy 聚合算子节点。
     * keySelector 和 aggregateFunction 打包为 KeyByConfig 存入 function 字段。
     * 返回新节点的 DataSet&lt;R&gt;。
     */
    public <K, R> DataSet<R> keyBy(KeySelector<T, K> keySelector,
                                    AggregateFunction<K, T, R> aggregateFunction) {
        KeyByConfig config = new KeyByConfig(keySelector, aggregateFunction);
        OperatorNode node = new OperatorNode(
                OperatorType.KEYBY,
                config,
                Collections.singletonList(currentNode.nodeId));
        env.getDagBuilder().addNode(node);
        return new DataSet<>(node, env);
    }

    /**
     * 注册一个 Union 合流节点，将当前流与 other 流合并为一条流。
     * 创建 OperatorNode(type=UNION, upstream=[currentNode.nodeId, other.currentNode.nodeId])。
     * 返回合并后新节点的 DataSet&lt;T&gt;。
     */
    public DataSet<T> union(DataSet<T> other) {
        OperatorNode node = new OperatorNode(
                OperatorType.UNION,
                null,
                Arrays.asList(currentNode.nodeId, other.currentNode.nodeId));
        env.getDagBuilder().addNode(node);
        return new DataSet<>(node, env);
    }

    /**
     * 注册一个 Sink 节点，终结当前流。无返回值。
     * 创建 OperatorNode(type=SINK, function=sinkFunction, upstream=[currentNode.nodeId])。
     */
    public void sink(SinkFunction<T> sinkFunction) {
        OperatorNode node = new OperatorNode(
                OperatorType.SINK,
                sinkFunction,
                Collections.singletonList(currentNode.nodeId));
        env.getDagBuilder().addNode(node);
    }

    /** 获取当前节点（供内部使用） */
    public OperatorNode getCurrentNode() {
        return currentNode;
    }
}
