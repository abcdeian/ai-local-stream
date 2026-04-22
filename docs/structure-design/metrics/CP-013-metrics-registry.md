# L4 — CP-013 MetricsRegistry

## 组件概述

所属领域：metrics

职责：维护 DAG 中所有节点的输入/输出计数器（`AtomicLong`）和 RPS 历史环形缓冲区（120 点，每 5 秒一个点）。提供 `incrementInput()` / `incrementOutput()` 供 runtime 上报，内置后台采样线程每 5 秒写入历史点，提供 `snapshot()` 供 web 查询。

## 内部结构

```
MetricsRegistry
 ├── List<String>               orderedNodeIds    // 按注册顺序记录 nodeId（即拓扑顺序）
 ├── Map<String, AtomicLong>    inputCounters     // nodeId → 输入总计数器（SOURCE 节点无此项）
 ├── Map<String, AtomicLong>    outputCounters    // nodeId → 输出总计数器（SINK 节点无此项）
 ├── Map<String, String>        nodeNames         // nodeId → nodeName（展示用）
 ├── Map<String, OperatorType>  nodeTypes         // nodeId → 节点类型
 ├── Map<String, RpsRingBuffer> inputRpsBuffers   // nodeId → 输入 RPS 环形缓冲区（非 SOURCE 节点）
 ├── Map<String, RpsRingBuffer> outputRpsBuffers  // nodeId → 输出 RPS 环形缓冲区（非 SINK 节点）
 └── ScheduledExecutorService   sampler           // 后台采样线程（每 5 秒触发一次）
```

### RpsRingBuffer（内部辅助类）

```
RpsRingBuffer
 ├── long[]      timestamps    // 环形数组，存储采样时间戳（ms），容量 120
 ├── long[]      rpsValues     // 环形数组，存储对应 RPS 值，容量 120
 ├── int         writeIndex    // 当前写入位置（0~119，循环）
 ├── int         size          // 当前已存储的有效点数（0~120）
 └── AtomicLong  windowCount   // 当前 5 秒窗口内的计数增量（每次 increment 累加）
```

## 功能小块

### MetricsRegistry

```java
public class MetricsRegistry {

    /**
     * 注册一个节点。由 StreamEnv 在启动初始化时按 JobGraph 拓扑顺序依次调用，单线程，无需加锁。
     * 注册顺序即拓扑顺序，MetricsRegistry 以 orderedNodeIds 列表记录，snapshot() 时按此顺序输出。
     * - SOURCE 节点：只注册 outputCounters + outputRpsBuffers（无 inputCounters）
     * - SINK   节点：只注册 inputCounters  + inputRpsBuffers （无 outputCounters）
     * - 其余节点：  同时注册输入和输出计数器与缓冲区
     */
    public void register(OperatorNode node);

    /**
     * 上报一次输入事件（ProcessorTask / SinkTask 每取出一条数据调用一次）。
     * inputCounters[nodeId] +1；inputRpsBuffers[nodeId].windowCount +1。
     * 线程安全（AtomicLong 保证）。
     * SOURCE 节点无 inputCounter，调用时静默忽略。
     */
    public void incrementInput(String nodeId);

    /**
     * 上报一次输出事件（SourceTask / ProcessorTask 每发出一条数据调用一次）。
     * outputCounters[nodeId] +1；outputRpsBuffers[nodeId].windowCount +1。
     * 线程安全（AtomicLong 保证）。
     * SINK 节点无 outputCounter，调用时静默忽略。
     */
    public void incrementOutput(String nodeId);

    /**
     * 启动后台采样线程（由 StreamEnv 在启动 JobExecutor 之前调用）。
     * 使用 ScheduledExecutorService 每 5 秒触发一次采样，
     * 将所有节点的当前窗口增量计算为 RPS 并写入对应 RpsRingBuffer。
     */
    public void start();

    /**
     * 停止后台采样线程（由 StreamEnv 在停止任务后调用）。
     * shutdownNow()，不阻塞。
     */
    public void stop();

    /**
     * 生成当前所有节点的指标快照。
     * 按 topologyOrder 升序遍历所有已注册节点，每个节点生成一条 NodeMetricsSummary：
     *   - inputCount  = inputCounters[nodeId].get()（SOURCE 节点固定为 0）
     *   - outputCount = outputCounters[nodeId].get()（SINK 节点固定为 0）
     *   - rpsHistory  = inputRpsBuffers 或 outputRpsBuffers 的历史点列表（按时间升序）
     *                   SOURCE 节点取 outputRpsBuffers；SINK 节点取 inputRpsBuffers；
     *                   其余节点取 outputRpsBuffers（代表该节点的处理吞吐）
     * 封装为 MetricsSnapshot 返回（弱一致性快照，不加全局锁）。
     */
    public MetricsSnapshot snapshot();
}
```

### RpsRingBuffer（内部辅助类）

```java
class RpsRingBuffer {
    private static final int CAPACITY = 120;   // 10 分钟 / 5 秒 = 120 个点

    private final long[]     timestamps  = new long[CAPACITY];
    private final long[]     rpsValues   = new long[CAPACITY];
    private int              writeIndex  = 0;
    private int              size        = 0;
    private final AtomicLong windowCount = new AtomicLong(0);

    /** 窗口计数 +1，由 increment 方法调用 */
    void increment();

    /**
     * 每 5 秒由后台采样线程调用。
     * 取出 windowCount 并重置为 0，计算 rps = count / 5（整数），
     * 写入环形数组（覆盖最旧的点），更新 writeIndex 和 size。
     */
    void sample(long nowMs);

    /**
     * 返回所有有效历史点，按时间升序（从最旧到最新）。
     * 返回 List<RpsDataPoint>，长度 0~120。
     */
    List<RpsDataPoint> history();
}
```

## 核心流程（伪代码）

```
MetricsRegistry.register(node):
  orderedNodeIds.add(node.nodeId)          // 保留注册顺序 = 拓扑顺序
  nodeNames.put(node.nodeId, node.name)
  nodeTypes.put(node.nodeId, node.type)
  if node.type != SOURCE:
    inputCounters.put(node.nodeId, new AtomicLong(0))
    inputRpsBuffers.put(node.nodeId, new RpsRingBuffer())
  if node.type != SINK:
    outputCounters.put(node.nodeId, new AtomicLong(0))
    outputRpsBuffers.put(node.nodeId, new RpsRingBuffer())

MetricsRegistry.incrementInput(nodeId):
  counter = inputCounters.get(nodeId)
  if counter != null:
    counter.incrementAndGet()
    inputRpsBuffers.get(nodeId).increment()

MetricsRegistry.incrementOutput(nodeId):
  counter = outputCounters.get(nodeId)
  if counter != null:
    counter.incrementAndGet()
    outputRpsBuffers.get(nodeId).increment()

MetricsRegistry.start():
  sampler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "metrics-sampler")
    t.setDaemon(true)
    return t
  })
  sampler.scheduleAtFixedRate(() -> {
    long nowMs = System.currentTimeMillis()
    for nodeId in inputRpsBuffers.keys():  inputRpsBuffers[nodeId].sample(nowMs)
    for nodeId in outputRpsBuffers.keys(): outputRpsBuffers[nodeId].sample(nowMs)
  }, 5, 5, TimeUnit.SECONDS)

MetricsRegistry.snapshot():
  nodeMetrics = new ArrayList<>()
  for i, nodeId in enumerate(orderedNodeIds):   // 按注册顺序 = 拓扑顺序遍历
    type     = nodeTypes[nodeId]
    inCount  = (inputCounters[nodeId] != null) ? inputCounters[nodeId].get() : 0
    outCount = (outputCounters[nodeId] != null) ? outputCounters[nodeId].get() : 0
    // RPS 历史：SOURCE 取 output，SINK 取 input，其余取 output（代表吞吐）
    rpsBuffer  = (type == SINK) ? inputRpsBuffers[nodeId] : outputRpsBuffers[nodeId]
    rpsHistory = rpsBuffer.history()
    nodeMetrics.add(new NodeMetricsSummary(
        nodeId, nodeNames[nodeId], type,
        i /*topologyOrder*/, inCount, outCount, rpsHistory))
  return new MetricsSnapshot(nodeMetrics, System.currentTimeMillis())

RpsRingBuffer.sample(nowMs):
  count = windowCount.getAndSet(0)
  rps   = count / 5                            // 整数，窗口 5 秒
  timestamps[writeIndex] = nowMs
  rpsValues[writeIndex]  = rps
  writeIndex = (writeIndex + 1) % CAPACITY
  if size < CAPACITY: size++

RpsRingBuffer.history():
  result = new ArrayList<>()
  if size == 0: return result
  startIdx = (size < CAPACITY) ? 0 : writeIndex   // 最旧点的位置
  for i in 0..size-1:
    idx = (startIdx + i) % CAPACITY
    result.add(new RpsDataPoint(timestamps[idx], rpsValues[idx]))
  return result
```

## 依赖约束

- 依赖 L2 定义：`NodeMetricsSummary`、`MetricsSnapshot`、`RpsDataPoint`、`OperatorNode`、`OperatorType`
- 仅依赖 JDK `java.util.concurrent.atomic.AtomicLong` 和 `ScheduledExecutorService`
- 线程安全：计数器增量用 `AtomicLong`；`sample()` 写入环形数组由单一采样线程执行，无并发写；`snapshot()` 为弱一致性快照（不加全局锁），满足展示用途
- 采样线程为 daemon 线程，不阻塞 JVM 退出
