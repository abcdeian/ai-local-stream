package com.localstream.common;

import java.util.List;

/**
 * DAG 中的一个逻辑节点，描述算子的类型和关联的用户函数。
 * 运行前不实例化线程，只描述拓扑结构。
 */
public class OperatorNode {
    public String nodeId;             // 全局唯一节点 ID（由 DagBuilder 赋值）
    public String name;               // 可读节点名称（如 "source-1"）
    public OperatorType type;         // 节点类型
    public Object function;           // 关联的用户函数（SourceFunction/FlatMapFunction/KeyByConfig 等）
    public List<String> upstreamIds;  // 上游节点 ID 列表

    public OperatorNode(OperatorType type, Object function, List<String> upstreamIds) {
        this.type = type;
        this.function = function;
        this.upstreamIds = upstreamIds;
    }
}
