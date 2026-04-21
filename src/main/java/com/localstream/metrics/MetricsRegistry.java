package com.localstream.metrics;

import com.localstream.common.MetricEntry;
import com.localstream.common.MetricsSnapshot;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.util.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 维护每个 SOURCE/SINK 节点的累计计数器（AtomicLong）和 RPS 滑动窗口（60 秒）。
 * 提供 increment() 供 runtime 上报，snapshot() 供 web 查询。
 */
public class MetricsRegistry {

    private static final Logger log = Logger.getLogger(MetricsRegistry.class);
    private static final long WINDOW_MS = 60_000L; // 60 秒窗口

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, String> nodeNames = new ConcurrentHashMap<>();
    private final Map<String, OperatorType> nodeTypes = new ConcurrentHashMap<>();
    private final Map<String, RpsWindow> rpsWindows = new ConcurrentHashMap<>();

    /**
     * 注册一个节点（仅 SOURCE/SINK）。
     * 由 StreamEnv 在启动初始化时调用，单线程，无需加锁。
     */
    public void register(OperatorNode node) {
        counters.put(node.nodeId, new AtomicLong(0));
        nodeNames.put(node.nodeId, node.name);
        nodeTypes.put(node.nodeId, node.type);
        rpsWindows.put(node.nodeId, new RpsWindow());
        log.info("MetricsRegistry: registered node [{}] type={}", node.name, node.type);
    }

    /**
     * 上报一次事件（SourceTask/SinkTask 每处理一条数据调用一次）。
     * 总计数器 +1；RPS 窗口计数 +1。线程安全（AtomicLong 保证）。
     */
    public void increment(String nodeId) {
        AtomicLong counter = counters.get(nodeId);
        if (counter != null) {
            counter.incrementAndGet();
            rpsWindows.get(nodeId).increment();
        }
    }

    /**
     * 生成当前所有节点的指标快照。
     * SOURCE 节点用 "read_count"/"read_rps"，SINK 节点用 "write_count"/"write_rps"。
     */
    public MetricsSnapshot snapshot() {
        List<MetricEntry> entries = new ArrayList<>();
        for (String nodeId : counters.keySet()) {
            boolean isSource = (nodeTypes.get(nodeId) == OperatorType.SOURCE);
            String countName = isSource ? "read_count" : "write_count";
            String rpsName = isSource ? "read_rps" : "write_rps";
            String name = nodeNames.getOrDefault(nodeId, nodeId);

            entries.add(new MetricEntry(nodeId, name, countName, counters.get(nodeId).get()));
            entries.add(new MetricEntry(nodeId, name, rpsName, rpsWindows.get(nodeId).currentRps()));
        }
        return new MetricsSnapshot(entries, System.currentTimeMillis());
    }

    /** RPS 滑动窗口（60s）内部辅助类 */
    static class RpsWindow {
        private final AtomicLong windowCount = new AtomicLong(0);
        private volatile long windowStartMs = System.currentTimeMillis();
        private volatile long lastRps = 0;

        void increment() {
            windowCount.incrementAndGet();
        }

        /**
         * 计算并返回当前 RPS（整数）。
         * 若距窗口开始 >= 60秒，则刷新并计算新 RPS；否则返回上次计算值。
         */
        long currentRps() {
            long now = System.currentTimeMillis();
            long elapsed = now - windowStartMs;
            if (elapsed >= WINDOW_MS) {
                long count = windowCount.getAndSet(0);
                windowStartMs = now;
                lastRps = elapsed > 0 ? count * 1000 / elapsed : 0;
            }
            return lastRps;
        }
    }
}
