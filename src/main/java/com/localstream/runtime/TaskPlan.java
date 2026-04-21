package com.localstream.runtime;

import com.localstream.common.CheckpointAckListener;
import com.localstream.common.Checkpointable;
import com.localstream.common.JobGraph;
import com.localstream.common.JobStartException;
import com.localstream.common.KeyByConfig;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.common.StreamConfig;
import com.localstream.metrics.MetricsRegistry;
import com.localstream.util.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将 JobGraph 编译为可执行的任务网络（Task 实例 + DataQueue 连接）。
 * 按拓扑序为每个节点创建 Task 和 DataQueue，维护 checkpointableMap。
 * 不负责线程管理——由 JobExecutor 承担。
 */
public class TaskPlan {

    private static final Logger log = Logger.getLogger(TaskPlan.class);

    private final List<SourceTask> sourceTasks = new ArrayList<>();
    private final List<ProcessorTask> processorTasks = new ArrayList<>();
    private final List<SinkTask> sinkTasks = new ArrayList<>();
    private final Map<String, Checkpointable> checkpointableMap = new HashMap<>();

    /**
     * 按 JobGraph 拓扑序创建所有任务实例和节点间队列。
     * 任何初始化异常包装为 JobStartException 抛出。
     */
    public TaskPlan(JobGraph graph, StreamConfig config, MetricsRegistry metricsRegistry)
            throws JobStartException {
        try {
            // 构建下游邻接表：nodeId → List<下游 nodeId>
            Map<String, List<String>> downstreams = new HashMap<>();
            for (OperatorNode node : graph.nodes) {
                if (node.upstreamIds != null) {
                    for (String upId : node.upstreamIds) {
                        downstreams.computeIfAbsent(upId, k -> new ArrayList<>()).add(node.nodeId);
                    }
                }
            }

            // 为每条边创建 DataQueue：upId → (downId → DataQueue)
            Map<String, Map<String, DataQueue>> edgeQueues = new HashMap<>();
            for (OperatorNode node : graph.nodes) {
                List<String> downs = downstreams.getOrDefault(node.nodeId, new ArrayList<>());
                Map<String, DataQueue> nodeQueues = new HashMap<>();
                for (String downId : downs) {
                    nodeQueues.put(downId, new DataQueue(config.queueCapacity, downId));
                }
                edgeQueues.put(node.nodeId, nodeQueues);
            }

            // 按拓扑序创建任务
            for (OperatorNode node : graph.nodes) {
                // 收集上游队列
                List<DataQueue> upQueues = new ArrayList<>();
                if (node.upstreamIds != null) {
                    for (String upId : node.upstreamIds) {
                        DataQueue q = edgeQueues.get(upId).get(node.nodeId);
                        upQueues.add(q);
                    }
                }
                // 收集下游队列
                List<DataQueue> downQueues = new ArrayList<>(
                        edgeQueues.getOrDefault(node.nodeId, new HashMap<>()).values());

                switch (node.type) {
                    case SOURCE: {
                        SourceTask task = new SourceTask(node, downQueues, metricsRegistry);
                        sourceTasks.add(task);
                        log.info("TaskPlan: created SourceTask [{}]", node.name);
                        break;
                    }
                    case SINK: {
                        DataQueue upQueue = upQueues.isEmpty() ? null : upQueues.get(0);
                        SinkTask task = new SinkTask(node, upQueue, metricsRegistry);
                        sinkTasks.add(task);
                        log.info("TaskPlan: created SinkTask [{}]", node.name);
                        break;
                    }
                    default: {
                        // FLATMAP, KEYBY, UNION
                        ProcessorTask task = new ProcessorTask(node, upQueues, downQueues);
                        processorTasks.add(task);
                        if (node.type == OperatorType.KEYBY) {
                            KeyByConfig cfg = (KeyByConfig) node.function;
                            checkpointableMap.put(node.nodeId, cfg.aggregateFunction);
                            log.info("TaskPlan: registered Checkpointable for KEYBY [{}]", node.name);
                        }
                        log.info("TaskPlan: created ProcessorTask [{}] type={}", node.name, node.type);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new JobStartException("TaskPlan init failed: " + e.getMessage(), e);
        }
    }

    /** 返回所有 SourceTask（供 CheckpointCoordinator 注入 Barrier 使用） */
    public List<SourceTask> getSourceTasks() {
        return sourceTasks;
    }

    /**
     * 返回非 Source 节点总数（ProcessorTask + SinkTask 数量之和）。
     * 供 CheckpointCoordinator 构造时传入，作为期望 ack 数量。
     */
    public int getNonSourceNodeCount() {
        return processorTasks.size() + sinkTasks.size();
    }

    /** 返回 nodeId → Checkpointable 映射（供 CheckpointCoordinator restore 阶段使用） */
    public Map<String, Checkpointable> getCheckpointables() {
        return checkpointableMap;
    }

    /**
     * 将 CheckpointAckListener 注入到所有 ProcessorTask 和 SinkTask。
     * 由 StreamEnv 在 TaskPlan 构造完成后、JobExecutor.startThreads() 之前调用。
     */
    public void setAckListener(CheckpointAckListener ackListener) {
        for (ProcessorTask t : processorTasks) t.setAckListener(ackListener);
        for (SinkTask t : sinkTasks) t.setAckListener(ackListener);
    }

    /**
     * 返回所有任务（SourceTask + ProcessorTask + SinkTask）的有序合并列表。
     * 供 JobExecutor 遍历创建线程。
     */
    public List<Runnable> getAllTasks() {
        List<Runnable> all = new ArrayList<>();
        all.addAll(sourceTasks);
        all.addAll(processorTasks);
        all.addAll(sinkTasks);
        return all;
    }
}
