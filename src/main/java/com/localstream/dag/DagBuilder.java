package com.localstream.dag;

import com.localstream.common.JobGraph;
import com.localstream.common.OperatorNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 接收 api 子域逐步注册的 OperatorNode，维护完整的节点集合与上下游边关系，
 * 最终产出供 DagValidator 校验的图结构。
 */
public class DagBuilder {

    private final List<OperatorNode> nodes = new ArrayList<>();
    private final Map<String, OperatorNode> index = new LinkedHashMap<>();
    /** 各类型节点的当前计数，用于生成可读名称 */
    private final Map<String, Integer> typeCount = new LinkedHashMap<>();

    /**
     * 注册一个新节点。
     * 生成全局唯一 nodeId（UUID 短串）和可读名称（如 "source-1"、"keyby-2"）。
     */
    public void addNode(OperatorNode node) {
        String typeName = node.type.name().toLowerCase();
        int count = typeCount.getOrDefault(typeName, 0) + 1;
        typeCount.put(typeName, count);
        // nodeId 与 name 保持一致，使用"类型-序号"格式（如 source-1、keyby-1）。
        // 确定性 nodeId 保证跨重启后 Checkpoint 状态能按 nodeId 正确恢复。
        node.name   = typeName + "-" + count;
        node.nodeId = node.name;
        nodes.add(node);
        index.put(node.nodeId, node);
    }

    /**
     * 构建并返回 JobGraph（此时节点为注册顺序，DagValidator 会重排为拓扑序）。
     */
    public JobGraph build() {
        return new JobGraph(new ArrayList<>(nodes), new LinkedHashMap<>(index));
    }

    /** 返回当前已注册的节点数量 */
    public int nodeCount() {
        return nodes.size();
    }
}
