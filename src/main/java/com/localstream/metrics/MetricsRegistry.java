package com.localstream.metrics;

import com.localstream.common.MetricsSnapshot;
import com.localstream.common.NodeMetricsSummary;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.common.RpsDataPoint;
import com.localstream.util.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 维护 DAG 中所有节点的输入/输出计数器（AtomicLong）和 RPS 历史环形缓冲区（120 点）。
 * 提供 incrementInput() / incrementOutput() 供 runtime 上报，
 * 内置后台采样线程每 5 秒写入历史点，提供 snapshot() 供 web 查询。
 */
public class MetricsRegistry {

    private static final Logger log = Logger.getLogger(MetricsRegistry.class);
    private static final int SAMPLE_INTERVAL_SEC = 5;

    /** 按注册顺序（= 拓扑顺序）存储 nodeId，LinkedHashMap 同时作为有序遍历基础 */
    private final List<String> orderedNodeIds = new ArrayList<>();
    private final Map<String, AtomicLong>   inputCounters  = new LinkedHashMap<>();
    private final Map<String, AtomicLong>   outputCounters = new LinkedHashMap<>();
    private final Map<String, String>       nodeNames      = new LinkedHashMap<>();
    private final Map<String, OperatorType> nodeTypes      = new LinkedHashMap<>();
    private final Map<String, RpsRingBuffer> inputRpsBuffers  = new LinkedHashMap<>();
    private final Map<String, RpsRingBuffer> outputRpsBuffers = new LinkedHashMap<>();

    private ScheduledExecutorService sampler;

    /**
     * 注册一个节点。由 StreamEnv 按 JobGraph 拓扑顺序依次调用，单线程，无需加锁。
     * 注册顺序即拓扑顺序，snapshot() 时按 orderedNodeIds 顺序输出。
     */
    public void register(OperatorNode node) {
        orderedNodeIds.add(node.nodeId);
        nodeNames.put(node.nodeId, node.name);
        nodeTypes.put(node.nodeId, node.type);
        if (node.type != OperatorType.SOURCE) {
            inputCounters.put(node.nodeId, new AtomicLong(0));
            inputRpsBuffers.put(node.nodeId, new RpsRingBuffer());
        }
        if (node.type != OperatorType.SINK) {
            outputCounters.put(node.nodeId, new AtomicLong(0));
            outputRpsBuffers.put(node.nodeId, new RpsRingBuffer());
        }
        log.info("MetricsRegistry: registered node [{}] type={}", node.name, node.type);
    }

    /**
     * 上报一次输入事件（ProcessorTask / SinkTask 每取出一条数据调用）。
     * SOURCE 节点无 inputCounter，调用时静默忽略。线程安全。
     */
    public void incrementInput(String nodeId) {
        AtomicLong counter = inputCounters.get(nodeId);
        if (counter != null) {
            counter.incrementAndGet();
            inputRpsBuffers.get(nodeId).increment();
        }
    }

    /**
     * 上报一次输出事件（SourceTask / ProcessorTask 每发出一条数据调用）。
     * SINK 节点无 outputCounter，调用时静默忽略。线程安全。
     */
    public void incrementOutput(String nodeId) {
        AtomicLong counter = outputCounters.get(nodeId);
        if (counter != null) {
            counter.incrementAndGet();
            outputRpsBuffers.get(nodeId).increment();
        }
    }

    /**
     * 启动后台采样线程（由 StreamEnv 在启动 JobExecutor 之前调用）。
     * 每 5 秒采样一次，将各节点窗口增量计算为 RPS 并写入环形缓冲区。
     */
    public void start() {
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "metrics-sampler");
            t.setDaemon(true);
            return t;
        };
        sampler = Executors.newSingleThreadScheduledExecutor(tf);
        sampler.scheduleAtFixedRate(() -> {
            long nowMs = System.currentTimeMillis();
            for (RpsRingBuffer buf : inputRpsBuffers.values())  buf.sample(nowMs);
            for (RpsRingBuffer buf : outputRpsBuffers.values()) buf.sample(nowMs);
        }, SAMPLE_INTERVAL_SEC, SAMPLE_INTERVAL_SEC, TimeUnit.SECONDS);
        log.info("MetricsRegistry sampler started");
    }

    /** 停止后台采样线程（由 StreamEnv 在任务停止后调用）。 */
    public void stop() {
        if (sampler != null) {
            sampler.shutdownNow();
        }
    }

    /**
     * 生成当前所有节点的指标快照。
     * 按 orderedNodeIds（注册顺序 = 拓扑顺序）遍历，弱一致性快照（不加全局锁）。
     */
    public MetricsSnapshot snapshot() {
        List<NodeMetricsSummary> nodeMetrics = new ArrayList<>();
        for (int i = 0; i < orderedNodeIds.size(); i++) {
            String nodeId = orderedNodeIds.get(i);
            OperatorType type = nodeTypes.get(nodeId);

            long inCount  = inputCounters.containsKey(nodeId)
                            ? inputCounters.get(nodeId).get() : 0L;
            long outCount = outputCounters.containsKey(nodeId)
                            ? outputCounters.get(nodeId).get() : 0L;

            // SOURCE 取 outputRps，SINK 取 inputRps，其余取 outputRps 代表吞吐
            RpsRingBuffer rpsBuffer = (type == OperatorType.SINK)
                    ? inputRpsBuffers.get(nodeId)
                    : outputRpsBuffers.get(nodeId);
            List<RpsDataPoint> rpsHistory = (rpsBuffer != null)
                    ? rpsBuffer.history()
                    : Collections.emptyList();

            nodeMetrics.add(new NodeMetricsSummary(
                    nodeId, nodeNames.get(nodeId), type,
                    i, inCount, outCount, rpsHistory));
        }
        return new MetricsSnapshot(nodeMetrics, System.currentTimeMillis());
    }

    // ——— 内部辅助类 ———

    /** RPS 历史环形缓冲区（120 点，每 5 秒一个点）。 */
    static class RpsRingBuffer {
        private static final int CAPACITY = 120;
        private final long[] timestamps = new long[CAPACITY];
        private final long[] rpsValues  = new long[CAPACITY];
        private int  writeIndex = 0;
        private int  size       = 0;
        private final AtomicLong windowCount = new AtomicLong(0);

        void increment() {
            windowCount.incrementAndGet();
        }

        /** 每 5 秒由采样线程调用：取出窗口增量计算 RPS，写入环形数组。 */
        void sample(long nowMs) {
            long count = windowCount.getAndSet(0);
            long rps   = count / SAMPLE_INTERVAL_SEC;
            timestamps[writeIndex] = nowMs;
            rpsValues[writeIndex]  = rps;
            writeIndex = (writeIndex + 1) % CAPACITY;
            if (size < CAPACITY) size++;
        }

        /** 返回所有有效历史点，按时间升序（从最旧到最新）。 */
        List<RpsDataPoint> history() {
            List<RpsDataPoint> result = new ArrayList<>(size);
            if (size == 0) return result;
            int startIdx = (size < CAPACITY) ? 0 : writeIndex;  // 最旧点位置
            for (int i = 0; i < size; i++) {
                int idx = (startIdx + i) % CAPACITY;
                result.add(new RpsDataPoint(timestamps[idx], rpsValues[idx]));
            }
            return result;
        }
    }
}
