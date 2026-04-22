package com.localstream.runtime;

import com.localstream.common.CheckpointBarrier;
import com.localstream.common.OperatorNode;
import com.localstream.common.SourceFunction;
import com.localstream.metrics.MetricsRegistry;
import com.localstream.util.Logger;

import java.util.List;

/**
 * 封装 SOURCE 节点的运行循环。
 * 持续调用 SourceFunction.fetch() 读取数据，写入所有下游 DataQueue，并通知 MetricsRegistry 计数。
 * 支持 Checkpoint Barrier 注入：CheckpointCoordinator 调用 injectBarrier() 后，
 * SourceTask 在下一轮循环将 Barrier 写入所有下游队列，数据流不中断。
 */
public class SourceTask implements Runnable {

    private static final Logger log = Logger.getLogger(SourceTask.class);

    private final OperatorNode node;
    private final List<DataQueue> downstreamQueues;
    private final MetricsRegistry metricsRegistry;
    private volatile CheckpointBarrier pendingBarrier = null;
    private volatile boolean running = true;

    public SourceTask(OperatorNode node, List<DataQueue> downstreamQueues,
                      MetricsRegistry metricsRegistry) {
        this.node = node;
        this.downstreamQueues = downstreamQueues;
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        log.info("SourceTask [{}] started", node.name);
        SourceFunction<Object> source = (SourceFunction<Object>) node.function;
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                // 优先处理待注入的 Barrier（夹在两条普通记录之间）
                CheckpointBarrier barrier = pendingBarrier;
                if (barrier != null) {
                    pendingBarrier = null;
                    for (DataQueue q : downstreamQueues) {
                        q.put(barrier);
                    }
                    continue;
                }

                Object record = source.fetch();
                if (record == null) {
                    Thread.sleep(1);
                    continue;
                }

                for (DataQueue q : downstreamQueues) {
                    q.put(record);
                }
                metricsRegistry.incrementOutput(node.nodeId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("SourceTask [{}] error: {}", node.name, e.getMessage());
            throw new RuntimeException(e);
        }
        log.info("SourceTask [{}] stopped", node.name);
    }

    /**
     * 由 CheckpointCoordinator 调用，触发 Barrier 注入。
     * volatile 写保证可见性，run 循环在下一轮检测到后写入所有下游队列。
     */
    public void injectBarrier(long checkpointId) {
        pendingBarrier = new CheckpointBarrier(checkpointId);
    }

    public void stop() {
        running = false;
    }

    public String getNodeId() {
        return node.nodeId;
    }

    public String getNodeName() {
        return node.name;
    }
}
