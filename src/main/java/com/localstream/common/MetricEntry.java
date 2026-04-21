package com.localstream.common;

/** 单条指标记录，描述某个节点的一个指标值。 */
public class MetricEntry {
    public final String nodeId;
    public final String nodeName;
    public final String metricName;   // "read_count" / "write_count" / "read_rps" / "write_rps"
    public final long metricValue;

    public MetricEntry(String nodeId, String nodeName, String metricName, long metricValue) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.metricName = metricName;
        this.metricValue = metricValue;
    }
}
