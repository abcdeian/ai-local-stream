# L2 — 通用定义 | LocalStream

> 本文档是 LocalStream 所有技术定义的**唯一来源**。L3/L4 文档引用本文档中的类型，不得在 L3/L4 中重新定义。

---

## 用户扩展接口（User-Facing SPI）

### SourceFunction\<T\>
用户实现此接口以提供数据来源。

```java
public interface SourceFunction<T> {
    /**
     * 系统持续循环调用此方法以读取数据。
     * 返回 null 表示当前无数据，系统短暂等待后再次调用。
     * 抛出 RuntimeException 时系统记录日志后继续调用。
     */
    T fetch();
}
```

### SinkFunction\<T\>
用户实现此接口以消费数据。

```java
public interface SinkFunction<T> {
    void invoke(T value);
}
```

### FlatMapFunction\<T, R\>
一对多转换函数，输入一条数据，输出 0 到 N 条数据。map 和 filter 语义均可通过此接口表达。

```java
public interface FlatMapFunction<T, R> {
    /** 返回空列表表示该条输入产生 0 条输出 */
    List<R> flatMap(T value);
}
```

### KeySelector\<T, K\>
从数据中提取分组键。

```java
public interface KeySelector<T, K> {
    K getKey(T value);
}
```

### AggregateFunction\<K, T, R\>
keyby 算子的聚合逻辑。`K` 为分组键类型，`T` 为输入数据类型，`R` 为输出结果类型。
函数实现方自行在内部维护聚合状态，每次 add 后返回该 key 产生的 0 到 N 条结果。
继承 `Checkpointable` 接口；若需要状态被 Checkpoint 保护，override `snapshotState()` / `restoreState()` 即可（默认空实现，不保护状态）。

```java
public interface AggregateFunction<K, T, R> extends Checkpointable {
    /** 将 key 和一条数据累加到内部状态，返回本次产生的结果列表（空列表表示无输出） */
    List<R> add(K key, T value);

    // snapshotState() / restoreState() 继承自 Checkpointable（默认空实现）
    // 若需要 Checkpoint 保护状态，在实现类中 override 即可
}
```

### KeyByConfig
打包 keyBy 算子所需的两个函数对象，由 `DataSet.keyBy()` 创建，存入 `OperatorNode.function` 字段，由 runtime 子域的 `ProcessorTask` 消费。

```java
public class KeyByConfig {
    public final KeySelector<?, ?> keySelector;
    public final AggregateFunction<?, ?, ?> aggregateFunction;

    public KeyByConfig(KeySelector<?, ?> keySelector,
                       AggregateFunction<?, ?, ?> aggregateFunction);
}
```

---

## DAG 模型定义

### OperatorType（枚举）
算子节点的类型标识。

```java
public enum OperatorType {
    SOURCE,    // 数据来源节点
    FLATMAP,   // 一对多转换节点（涵盖 map / filter 语义，由 FlatMapFunction 实现方控制输出条数）
    KEYBY,     // 按键聚合节点
    UNION,     // 合流节点
    SINK       // 数据输出节点
}
```

### OperatorNode
DAG 中的一个逻辑节点，描述算子的类型和关联的用户函数（运行前不实例化线程）。

```java
public class OperatorNode {
    String nodeId;           // 全局唯一节点 ID（自动生成，由 DagBuilder 赋值）
    String name;             // 可读节点名称（由 DagBuilder 按类型+序号自动生成，如 "source-1"）
    OperatorType type;       // 节点类型
    Object function;         // 关联的用户函数（SourceFunction / FlatMapFunction 等）
    List<String> upstreamIds; // 上游节点 ID 列表
}
```

### JobGraph
DAG 拓扑的完整表示，经过合法性校验后由 dag 子域产出。

```java
public class JobGraph {
    List<OperatorNode> nodes;              // 所有节点（拓扑排序后的顺序）
    Map<String, OperatorNode> nodeIndex;   // nodeId → OperatorNode 快速查找
}
```

---

## Checkpoint 接口定义

### Checkpointable
状态快照接口，由 checkpoint 子域统一调用。实现方通过 override `snapshotState()` / `restoreState()` 参与 Checkpoint 状态保护；
**不包含 `getNodeId()`**——nodeId 由 runtime 通过持有的 `OperatorNode` 实例在运行时获取，不耦合进此接口。
当前实现方：`AggregateFunction`（KEYBY 节点持有，系统以 `node.nodeId` 为 key 注册）。

```java
public interface Checkpointable {
    /**
     * 获取当前状态快照，由系统在 Checkpoint 时调用。
     * 默认实现：返回空 Map（无状态）。
     */
    default Map<String, String> snapshotState() {
        return Collections.emptyMap();
    }

    /**
     * 系统在任务恢复时调用，将历史快照注入实现方，由实现方恢复内部状态。
     * 默认实现：不做任何操作。
     */
    default void restoreState(Map<String, String> state) {}
}
```

---

## State 存储抽象

### StateBackend
状态存储的抽象接口，当前由 `LocalDiskStateBackend` 实现。

```java
public interface StateBackend {
    /**
     * 将一次 Checkpoint 的全量状态持久化。
     * @param checkpointId  本次 Checkpoint 版本号
     * @param states        nodeId → 状态快照（value 已由节点序列化为 String）
     */
    void save(long checkpointId, Map<String, Map<String, String>> states) throws Exception;

    /**
     * 加载指定版本的全量状态。
     * @return nodeId → 状态快照；若版本不存在或文件损坏，抛出 Exception
     */
    Map<String, Map<String, String>> load(long checkpointId) throws Exception;

    /**
     * 返回当前已持久化的最新 Checkpoint ID；若无任何记录则返回 -1。
     */
    long latestCheckpointId() throws Exception;

    /**
     * 返回指定 checkpointId 对应的存储路径字符串（供 CheckpointInfo 展示用）。
     */
    String pathOf(long checkpointId);
}
```

---

## Metrics 定义

### MetricEntry
单条指标记录，描述某个节点的一个指标值。

```java
public class MetricEntry {
    String nodeId;       // 节点唯一 ID
    String nodeName;     // 节点名称（便于展示）
    String metricName;   // 指标名称，如 "read_count"、"write_count"
    long metricValue;    // 指标值
}
```

### MetricsSnapshot
一次指标查询的快照结果，包含所有节点的所有指标条目。

```java
public class MetricsSnapshot {
    /** 所有节点的指标条目列表 */
    List<MetricEntry> entries;
    /** 快照采集时的 Unix 毫秒时间戳 */
    long snapshotTime;
}
```

---

## StreamEnv 配置

### StreamConfig
用户在创建 `StreamEnv` 时可选择性地传入配置，控制系统行为。

```java
public class StreamConfig {
    /** 任务名称，默认 "LocalStreamJob" */
    String jobName = "LocalStreamJob";
    /** Checkpoint 触发周期（毫秒），默认 60000（1分钟） */
    long checkpointIntervalMs = 60_000L;
    /** Checkpoint 存储根路径，默认 "./checkpoints" */
    String checkpointDir = "./checkpoints";
    /** 节点间队列容量，默认 1024 */
    int queueCapacity = 1024;
    /** 是否启用 Checkpoint，默认 true */
    boolean checkpointEnabled = true;
    /** Checkpoint ack 等待超时时间（毫秒），默认 30000（30秒） */
    long checkpointAckTimeoutMs = 30_000L;
    /** 日志文件路径，默认 "./logs/localstream.log" */
    String logFilePath = "./logs/localstream.log";
    /** Web 服务配置 */
    WebConfig webConfig = new WebConfig();
}
```

---

## Web 展示定义

### TaskInfo
任务运行时信息，由 runtime 子域产出，供 web 子域查询展示。包含任务基本状态和完整配置快照。

```java
public class TaskInfo {
    String jobName;              // 任务名称
    long startTimeMs;            // 任务启动时间（Unix 毫秒）
    String status;               // 任务状态：RUNNING / STOPPED / FAILED
    int totalNodes;              // DAG 节点总数
    StreamConfig config;         // 任务启动时使用的完整配置
}
```

### CheckpointInfo
Checkpoint 运行状态摘要，由 checkpoint 子域产出，供 web 子域查询展示。

```java
public class CheckpointInfo {
    long totalCount;             // 已成功完成的 Checkpoint 总次数
    long latestCheckpointId;     // 最近一次成功的 Checkpoint ID（无则为 -1）
    String latestStatus;         // 最近一次 Checkpoint 状态："SUCCESS" / "FAILED" / "NONE"
    String latestStoragePath;    // 最近一次成功 Checkpoint 的存储路径
    long latestTimestampMs;      // 最近一次成功 Checkpoint 的完成时间（Unix 毫秒）
}
```

### WebConfig
Web 服务的配置，作为 `StreamConfig` 的组成部分。

```java
public class WebConfig {
    /** Web 服务监听端口，默认 8080 */
    int port = 8080;
    /** 是否启用 Web 服务，默认 true */
    boolean enabled = true;
    /** 前端页面数据自动刷新间隔（秒），默认 5 */
    int refreshIntervalSeconds = 5;
    /** Web 日志页面最大展示行数，默认 500 */
    int logTailLines = 500;
}
```

---

## Checkpoint 流控定义

### CheckpointBarrier
系统内部流控记录，与普通数据记录共用同一 `DataQueue`，承载 Checkpoint ID 随数据流传播。
算子收到 Barrier 时触发本节点状态快照，无状态算子直接转发。**用户代码不可见，不可创建。**

```java
public final class CheckpointBarrier {
    /** Checkpoint 版本号，由 CheckpointCoordinator 分配 */
    final long checkpointId;

    public CheckpointBarrier(long checkpointId);
    public long getCheckpointId();
}
```

### CheckpointAckListener
Checkpoint ack 回调接口，由 `CheckpointCoordinator` 实现，注入到 `ProcessorTask` 和 `SinkTask` 中。
算子完成本节点快照后通过此接口上报 Coordinator，避免 runtime 子域直接依赖 checkpoint 子域。

```java
public interface CheckpointAckListener {
    /**
     * 算子处理完 CheckpointBarrier 后回调，上报快照状态。
     * @param checkpointId  本次 Checkpoint 版本号
     * @param nodeId        当前节点 ID
     * @param stateSnapshot 当前节点状态快照（无状态节点传空 Map）
     */
    void onBarrierProcessed(long checkpointId, String nodeId,
                            Map<String, String> stateSnapshot);
}
```

---

## 异常体系

| 异常类 | 类型 | 触发场景 |
|--------|------|----------|
| `DagValidationException` | 运行时异常 | DAG 校验失败（含环路、节点缺失等） |
| `JobStartException` | 运行时异常 | 任务启动时节点初始化失败 |
