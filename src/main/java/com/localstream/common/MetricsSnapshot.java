package com.localstream.common;

import java.util.List;

/** 一次指标查询的快照结果，包含所有节点按拓扑顺序排列的汇总指标。 */
public class MetricsSnapshot {
    public final List<NodeMetricsSummary> nodeMetrics;
    public final long snapshotTime;  // Unix 毫秒时间戳

    public MetricsSnapshot(List<NodeMetricsSummary> nodeMetrics, long snapshotTime) {
        this.nodeMetrics = nodeMetrics;
        this.snapshotTime = snapshotTime;
    }
}
