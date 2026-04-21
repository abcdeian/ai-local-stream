package com.localstream.runtime;

import com.localstream.common.CheckpointAckListener;
import com.localstream.common.CheckpointBarrier;
import com.localstream.common.FlatMapFunction;
import com.localstream.common.KeyByConfig;
import com.localstream.common.KeySelector;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.util.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 封装 FLATMAP / KEYBY / UNION 节点的运行循环。
 * 持续从上游 DataQueue 消费数据，根据节点类型执行对应算子逻辑，将结果写入所有下游 DataQueue。
 * 处理 CheckpointBarrier：收到 Barrier 后执行状态快照，通过 CheckpointAckListener 上报，再转发。
 */
public class ProcessorTask implements Runnable {

    private static final Logger log = Logger.getLogger(ProcessorTask.class);

    private final OperatorNode node;
    private final List<DataQueue> upstreamQueues;
    private final List<DataQueue> downstreamQueues;
    private volatile CheckpointAckListener ackListener = null;
    /** UNION 专用：已收到 Barrier 的上游队列 → checkpointId */
    private final Map<DataQueue, Long> barrierAlignState = new HashMap<>();
    private volatile boolean running = true;

    public ProcessorTask(OperatorNode node,
                         List<DataQueue> upstreamQueues,
                         List<DataQueue> downstreamQueues) {
        this.node = node;
        this.upstreamQueues = upstreamQueues;
        this.downstreamQueues = downstreamQueues;
    }

    @Override
    public void run() {
        log.info("ProcessorTask [{}] started, type={}", node.name, node.type);
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                switch (node.type) {
                    case FLATMAP: processFlatMap(); break;
                    case KEYBY:   processKeyBy();   break;
                    case UNION:   processUnion();   break;
                    default:
                        throw new IllegalStateException("Unknown processor type: " + node.type);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("ProcessorTask [{}] error: {}", node.name, e.getMessage());
            throw new RuntimeException(e);
        }
        log.info("ProcessorTask [{}] stopped", node.name);
    }

    public void stop() {
        running = false;
    }

    public String getNodeId() {
        return node.nodeId;
    }

    /**
     * 注入 CheckpointAckListener（由 StreamEnv 通过 TaskPlan.setAckListener() 注入）。
     * 未注入时 Barrier 仍会被透传，但不发送 ack（Checkpoint 未启用时的降级行为）。
     */
    public void setAckListener(CheckpointAckListener ackListener) {
        this.ackListener = ackListener;
    }

    // ——— 私有处理方法 ———

    @SuppressWarnings("unchecked")
    private void processFlatMap() throws InterruptedException {
        Object record = upstreamQueues.get(0).take();
        if (record instanceof CheckpointBarrier) {
            handleBarrier((CheckpointBarrier) record);
            return;
        }
        FlatMapFunction<Object, Object> fn = (FlatMapFunction<Object, Object>) node.function;
        List<Object> results = fn.flatMap(record);
        if (results != null) {
            for (Object r : results) {
                for (DataQueue q : downstreamQueues) q.put(r);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void processKeyBy() throws InterruptedException {
        Object record = upstreamQueues.get(0).take();
        if (record instanceof CheckpointBarrier) {
            handleBarrier((CheckpointBarrier) record);
            return;
        }
        KeyByConfig config = (KeyByConfig) node.function;
        KeySelector<Object, Object> keySelector = (KeySelector<Object, Object>) config.keySelector;
        Object key = keySelector.getKey(record);
        @SuppressWarnings("rawtypes")
        List results = ((com.localstream.common.AggregateFunction) config.aggregateFunction).add(key, record);
        if (results != null) {
            for (Object r : results) {
                for (DataQueue q : downstreamQueues) q.put(r);
            }
        }
    }

    private void processUnion() throws InterruptedException {
        for (DataQueue queue : upstreamQueues) {
            if (barrierAlignState.containsKey(queue)) continue; // 该队列已对齐，暂停消费

            Object record = queue.poll(1); // 非阻塞 poll（1ms 超时）
            if (record == null) continue;

            if (record instanceof CheckpointBarrier) {
                CheckpointBarrier barrier = (CheckpointBarrier) record;
                barrierAlignState.put(queue, barrier.getCheckpointId());

                if (barrierAlignState.size() == upstreamQueues.size()) {
                    // 所有上游 Barrier 已对齐
                    long checkpointId = barrier.getCheckpointId();
                    if (ackListener != null) {
                        ackListener.onBarrierProcessed(checkpointId, node.nodeId,
                                Collections.emptyMap()); // UNION 无状态
                    }
                    for (DataQueue q : downstreamQueues) {
                        q.put(new CheckpointBarrier(checkpointId));
                    }
                    barrierAlignState.clear(); // 对齐完成，恢复所有队列消费
                }
            } else {
                for (DataQueue q : downstreamQueues) q.put(record);
            }
        }
    }

    /** 线性节点（FLATMAP/KEYBY）通用 Barrier 处理 */
    private void handleBarrier(CheckpointBarrier barrier) throws InterruptedException {
        Map<String, String> stateMap;
        if (node.type == OperatorType.KEYBY) {
            KeyByConfig config = (KeyByConfig) node.function;
            stateMap = config.aggregateFunction.snapshotState();
        } else {
            stateMap = Collections.emptyMap();
        }
        if (ackListener != null) {
            ackListener.onBarrierProcessed(barrier.getCheckpointId(), node.nodeId, stateMap);
        }
        // 转发到所有下游（分叉时广播）
        for (DataQueue q : downstreamQueues) {
            q.put(barrier);
        }
    }
}
