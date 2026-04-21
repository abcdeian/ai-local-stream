package com.localstream.common;

import java.util.List;

/** 一次指标查询的快照结果，包含所有节点的所有指标条目。 */
public class MetricsSnapshot {
    public final List<MetricEntry> entries;
    public final long snapshotTime;  // Unix 毫秒时间戳

    public MetricsSnapshot(List<MetricEntry> entries, long snapshotTime) {
        this.entries = entries;
        this.snapshotTime = snapshotTime;
    }
}
