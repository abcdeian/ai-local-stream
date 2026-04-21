package com.localstream.optimizer;

import com.localstream.common.JobGraph;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.util.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * 对已通过校验的 JobGraph 执行死节点消除（Dead Node Elimination）优化。
 * 从所有 SINK 节点出发，沿 upstreamIds 反向遍历，标记所有"有效节点"；
 * 将所有未被标记的节点从 JobGraph 中移除。
 */
public class DagOptimizer {

    private static final Logger log = Logger.getLogger(DagOptimizer.class);

    /**
     * 对 JobGraph 执行死节点消除优化（原地修改）。
     * 前提：graph 已经过 DagValidator.validate() 校验（保证无环且含 SINK）。
     */
    public void optimize(JobGraph graph) {
        Set<String> liveIds = collectLiveNodeIds(graph);

        List<OperatorNode> deadNodes = new ArrayList<>();
        for (OperatorNode node : graph.nodes) {
            if (!liveIds.contains(node.nodeId)) {
                deadNodes.add(node);
            }
        }

        if (deadNodes.isEmpty()) {
            return; // 无死节点，无需优化
        }

        for (OperatorNode dead : deadNodes) {
            log.info("DagOptimizer: removing dead node [{}] type={}", dead.nodeId, dead.type);
        }

        graph.nodes.removeAll(deadNodes);
        graph.nodeIndex.keySet().retainAll(liveIds);
    }

    /** 反向 BFS：从 SINK 出发，沿 upstreamIds 收集所有可达节点 ID */
    private Set<String> collectLiveNodeIds(JobGraph graph) {
        Set<String> liveIds = new HashSet<>();
        Queue<OperatorNode> queue = new LinkedList<>();

        for (OperatorNode node : graph.nodes) {
            if (node.type == OperatorType.SINK) {
                queue.add(node);
            }
        }

        while (!queue.isEmpty()) {
            OperatorNode cur = queue.poll();
            if (liveIds.contains(cur.nodeId)) continue;
            liveIds.add(cur.nodeId);

            if (cur.upstreamIds != null) {
                for (String upId : cur.upstreamIds) {
                    OperatorNode upstream = graph.nodeIndex.get(upId);
                    if (upstream != null && !liveIds.contains(upstream.nodeId)) {
                        queue.add(upstream);
                    }
                }
            }
        }
        return liveIds;
    }
}
