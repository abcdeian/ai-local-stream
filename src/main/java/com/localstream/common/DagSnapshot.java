package com.localstream.common;

import java.util.List;

/** DAG 拓扑快照，供 /api/dag 接口返回，包含所有节点按拓扑顺序排列的视图。 */
public class DagSnapshot {
    public final List<DagNodeView> nodes;  // 所有节点，按拓扑排序（从 Source 到 Sink）

    public DagSnapshot(List<DagNodeView> nodes) {
        this.nodes = nodes;
    }
}
