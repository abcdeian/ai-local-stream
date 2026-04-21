package com.localstream.common;

import java.util.List;
import java.util.Map;

/**
 * DAG 拓扑的完整表示，经过合法性校验后由 dag 子域产出。
 * nodes 在 DagValidator 校验通过后按拓扑序排列（SOURCE 在前，SINK 在后）。
 */
public class JobGraph {
    public List<OperatorNode> nodes;             // 所有节点（拓扑排序后的顺序）
    public Map<String, OperatorNode> nodeIndex;  // nodeId → OperatorNode 快速查找

    public JobGraph(List<OperatorNode> nodes, Map<String, OperatorNode> nodeIndex) {
        this.nodes = nodes;
        this.nodeIndex = nodeIndex;
    }
}
