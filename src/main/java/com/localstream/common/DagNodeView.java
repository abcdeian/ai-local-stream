package com.localstream.common;

import java.util.List;

/** DAG 中单个节点的只读视图，用于 Web 端渲染拓扑图。 */
public class DagNodeView {
    public final String nodeId;
    public final String name;
    public final OperatorType type;
    public final List<String> upstreamIds;  // SOURCE 节点为空列表

    public DagNodeView(String nodeId, String name, OperatorType type, List<String> upstreamIds) {
        this.nodeId = nodeId;
        this.name = name;
        this.type = type;
        this.upstreamIds = upstreamIds;
    }
}
