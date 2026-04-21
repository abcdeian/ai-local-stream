package com.localstream.dag;

import com.localstream.common.DagValidationException;
import com.localstream.common.JobGraph;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * 对 DagBuilder 产出的 JobGraph 执行合法性校验：
 * ① 检查图是否为空；
 * ② 拓扑排序（Kahn 算法）检测有向环路；
 * ③ 校验图中至少存在一个 SINK 节点。
 * 校验通过后将 graph.nodes 重排为拓扑顺序（SOURCE 在前，SINK 在后）。
 */
public class DagValidator {

    /**
     * 对 JobGraph 执行合法性校验。
     * 校验通过：将 graph.nodes 替换为拓扑排序后的有序列表。
     * 任意校验失败：抛出 DagValidationException。
     */
    public void validate(JobGraph graph) {
        if (graph.nodes == null || graph.nodes.isEmpty()) {
            throw new DagValidationException("DAG is empty");
        }
        List<OperatorNode> sorted = kahnSort(graph);
        if (sorted.size() != graph.nodes.size()) {
            throw new DagValidationException("DAG contains cycle");
        }
        boolean hasSink = sorted.stream().anyMatch(n -> n.type == OperatorType.SINK);
        if (!hasSink) {
            throw new DagValidationException("DAG has no sink node");
        }
        // 原地替换为拓扑序
        graph.nodes = sorted;
    }

    /** Kahn 算法核心：返回拓扑排序结果（如有环则结果长度 < 总节点数） */
    private List<OperatorNode> kahnSort(JobGraph graph) {
        // 构建出度邻接表（每个节点的下游列表）
        Map<String, List<String>> outEdges = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();

        for (OperatorNode node : graph.nodes) {
            inDegree.put(node.nodeId, node.upstreamIds == null ? 0 : node.upstreamIds.size());
            if (!outEdges.containsKey(node.nodeId)) {
                outEdges.put(node.nodeId, new ArrayList<>());
            }
            if (node.upstreamIds != null) {
                for (String upId : node.upstreamIds) {
                    outEdges.computeIfAbsent(upId, k -> new ArrayList<>()).add(node.nodeId);
                }
            }
        }

        // 将所有入度为 0 的节点入队
        Queue<OperatorNode> queue = new LinkedList<>();
        for (OperatorNode node : graph.nodes) {
            if (inDegree.get(node.nodeId) == 0) {
                queue.add(node);
            }
        }

        List<OperatorNode> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            OperatorNode cur = queue.poll();
            result.add(cur);
            List<String> downs = outEdges.getOrDefault(cur.nodeId, new ArrayList<>());
            for (String downId : downs) {
                int newDegree = inDegree.get(downId) - 1;
                inDegree.put(downId, newDegree);
                if (newDegree == 0) {
                    queue.add(graph.nodeIndex.get(downId));
                }
            }
        }
        return result;
    }
}
