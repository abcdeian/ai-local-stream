package com.localstream.runtime;

import com.localstream.common.CheckpointAckListener;
import com.localstream.common.CheckpointBarrier;
import com.localstream.common.OperatorNode;
import com.localstream.common.SinkFunction;
import com.localstream.metrics.MetricsRegistry;
import com.localstream.util.Logger;

import java.util.Collections;

/**
 * 封装 SINK 节点的运行循环。
 * 持续从上游 DataQueue 消费数据，调用 SinkFunction.invoke() 写出，并通知 MetricsRegistry 计数。
 * 收到 CheckpointBarrier 时向 Coordinator ack，不转发 Barrier（无下游）。
 */
public class SinkTask implements Runnable {

    private static final Logger log = Logger.getLogger(SinkTask.class);

    private final OperatorNode node;
    private final DataQueue upstreamQueue;
    private final MetricsRegistry metricsRegistry;
    private volatile CheckpointAckListener ackListener = null;
    private volatile boolean running = true;

    public SinkTask(OperatorNode node, DataQueue upstreamQueue,
                    MetricsRegistry metricsRegistry) {
        this.node = node;
        this.upstreamQueue = upstreamQueue;
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        log.info("SinkTask [{}] started", node.name);
        SinkFunction<Object> sink = (SinkFunction<Object>) node.function;
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                Object record = upstreamQueue.take();

                if (record instanceof CheckpointBarrier) {
                    CheckpointBarrier barrier = (CheckpointBarrier) record;
                    // SinkTask 是 pipeline 末端，无需转发 Barrier；SinkTask 无状态，传空 Map
                    if (ackListener != null) {
                        ackListener.onBarrierProcessed(barrier.getCheckpointId(),
                                node.nodeId, Collections.emptyMap());
                    }
                } else {
                    sink.invoke(record);
                    metricsRegistry.increment(node.nodeId);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("SinkTask [{}] error: {}", node.name, e.getMessage());
            throw new RuntimeException(e);
        }
        log.info("SinkTask [{}] stopped", node.name);
    }

    public void stop() {
        running = false;
    }

    public String getNodeId() {
        return node.nodeId;
    }

    /**
     * 注入 CheckpointAckListener（由 StreamEnv 通过 TaskPlan.setAckListener() 注入）。
     * 未注入时 Barrier 会被静默忽略（Checkpoint 未启用时的降级行为）。
     */
    public void setAckListener(CheckpointAckListener ackListener) {
        this.ackListener = ackListener;
    }
}
