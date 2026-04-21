# L4 — CP-013 MetricsRegistry

## 组件概述

所属领域：metrics

职责：维护每个 SOURCE/SINK 节点的累计计数器（`AtomicLong`）和 RPS 滑动窗口（60 秒）。提供 `increment()` 供 runtime 上报，`snapshot()` 供 web 查询。

## 内部结构

```
MetricsRegistry
 ├── Map<String, AtomicLong>    counters    // nodeId → 总计数器
 ├── Map<String, String>        nodeNames   // nodeId → nodeName（展示用）
 ├── Map<String, OperatorType>  nodeTypes   // nodeId → 节点类型（SOURCE/SINK，区分 read/write 指标名）
 └── Map<String, RpsWindow>     rpsWindows  // nodeId → RPS 滑动窗口（60s）
```

### RpsWindow（内部辅助类）

```
RpsWindow
 ├── AtomicLong windowCount    // 当前窗口内的计数增量
 ├── long windowStartMs        // 窗口开始时间
 └── long lastRps              // 上一次计算出的 RPS（窗口未满时返回此值）
```

## 功能小块

### MetricsRegistry

```java
public class MetricsRegistry {

    /**
     * 注册一个节点（仅 SOURCE/SINK）。
     * 从 OperatorNode 中取 nodeId、name、type，分别初始化
     * AtomicLong 计数器、nodeName 映射、nodeType 映射和 RpsWindow（60s 窗口）。
     * 由 StreamEnv 在启动初始化时调用，单线程，无需加锁。
     */
    public void register(OperatorNode node);

    /**
     * 上报一次事件（SourceTask/SinkTask 每处理一条数据调用一次）。
     * 总计数器 +1；RPS 窗口计数 +1。
     * 线程安全（AtomicLong 保证）。
     */
    public void increment(String nodeId);

    /**
     * 生成当前所有节点的指标快照。
     * 每个节点生成两条 MetricEntry：
     *   1. metricName="read_count"/"write_count", metricValue=总计数
     *   2. metricName="read_rps"/"write_rps",    metricValue=当前 RPS（整数）
     * SOURCE 节点用 "read_count"/"read_rps"，SINK 节点用 "write_count"/"write_rps"。
     * 封装为 MetricsSnapshot 返回。
     */
    public MetricsSnapshot snapshot();
}
```

### RpsWindow（内部辅助类）

```java
class RpsWindow {
    private final AtomicLong windowCount = new AtomicLong(0);
    private volatile long windowStartMs = System.currentTimeMillis();
    private volatile long lastRps = 0;

    void increment();

    /**
     * 计算并返回当前 RPS（整数）。
     * 若距窗口开始 >= 60_000ms（60秒），则 RPS = windowCount * 1000 / elapsed（换算为每秒），
     * 重置窗口（windowCount=0, windowStartMs=now），更新 lastRps。
     * 否则返回 lastRps（窗口未满 60 秒，用上次计算值）。
     */
    long currentRps();
}
```

## 核心流程（伪代码）

```
MetricsRegistry.register(node):
  counters.put(node.nodeId, new AtomicLong(0))
  nodeNames.put(node.nodeId, node.name)
  nodeTypes.put(node.nodeId, node.type)
  rpsWindows.put(node.nodeId, new RpsWindow())

MetricsRegistry.increment(nodeId):
  counters.get(nodeId).incrementAndGet()
  rpsWindows.get(nodeId).increment()

MetricsRegistry.snapshot():
  entries = new ArrayList<>()
  for nodeId in counters.keys():
    isSource  = (nodeTypes.get(nodeId) == SOURCE)
    countName = isSource ? "read_count"  : "write_count"
    rpsName   = isSource ? "read_rps"    : "write_rps"

    entries.add(new MetricEntry(nodeId, nodeNames[nodeId], countName, counters[nodeId].get()))
    entries.add(new MetricEntry(nodeId, nodeNames[nodeId], rpsName,   rpsWindows[nodeId].currentRps()))

  return new MetricsSnapshot(entries, System.currentTimeMillis())

RpsWindow.currentRps():
  elapsed = System.currentTimeMillis() - windowStartMs
  if elapsed >= 60_000:                          // 60 秒窗口到期
    count = windowCount.getAndSet(0)
    windowStartMs = System.currentTimeMillis()
    lastRps = count * 1000 / elapsed             // 整数 RPS（约等于 count / 60）
  return lastRps
```

## 依赖约束

- 依赖 L2 定义：`MetricEntry`、`MetricsSnapshot`、`OperatorNode`、`OperatorType`
- 仅依赖 JDK `java.util.concurrent.atomic.AtomicLong`
- 线程安全：计数器和 RPS 增量均用 `AtomicLong`；`snapshot()` 为弱一致性快照（不加全局锁），满足展示用途
