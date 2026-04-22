package com.localstream.common;

import java.util.List;

/**
 * 单个节点的完整指标汇总：拓扑顺序、输入/输出总计数、最近 10 分钟 RPS 历史。
 * 覆盖 DAG 中所有节点（SOURCE / FLATMAP / KEYBY / UNION / SINK）。
 */
public class NodeMetricsSummary {
    public final String nodeId;
    public final String nodeName;
    public final OperatorType type;
    public final int topologyOrder;       // 0-based，从 Source 到 Sink 升序
    public final long inputCount;         // 累计接收条数（SOURCE 节点为 0）
    public final long outputCount;        // 累计发出条数（SINK 节点为 0）
    public final List<RpsDataPoint> rpsHistory;  // 最多 120 个点，按时间升序

    public NodeMetricsSummary(String nodeId, String nodeName, OperatorType type,
                              int topologyOrder, long inputCount, long outputCount,
                              List<RpsDataPoint> rpsHistory) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.type = type;
        this.topologyOrder = topologyOrder;
        this.inputCount = inputCount;
        this.outputCount = outputCount;
        this.rpsHistory = rpsHistory;
    }
}
