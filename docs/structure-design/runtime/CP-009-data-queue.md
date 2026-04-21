# L4 — CP-009 DataQueue

## 组件概述

所属领域：runtime

职责：有界阻塞队列的包装，连接 DAG 中相邻节点的数据通道。写满时写入方阻塞（天然背压），读空时读取方阻塞。
队列元素类型为 `Object`，可同时承载**普通数据记录**和**`CheckpointBarrier`**；消费方通过 `instanceof` 判断类型。
fanout 场景（一写多读）由 TaskPlan 为每个下游节点各创建一个独立 DataQueue，SourceTask/ProcessorTask 逐一写入。

## 内部结构

```
DataQueue
 └── ArrayBlockingQueue<Object> queue   // JDK 有界阻塞队列，容量由 queueCapacity 决定
```

## 功能小块

### DataQueue

```java
public class DataQueue {

    private final ArrayBlockingQueue<Object> queue;
    private final String queueId;   // 关联的下游节点 nodeId（调试用）

    public DataQueue(int capacity, String queueId);

    /**
     * 向队列写入一条数据。
     * 队列满时阻塞，直到有空位（背压）。
     * 线程被中断时抛出 InterruptedException，向上传播。
     */
    public void put(Object record) throws InterruptedException;

    /**
     * 从队列读取一条数据，队列空时阻塞。
     * 线程被中断时抛出 InterruptedException。
     */
    public Object take() throws InterruptedException;

    /**
     * 尝试从队列读取一条数据，超过 timeoutMs 毫秒无数据则返回 null。
     * 用于 UNION 节点的轮询消费，避免单队列长期阻塞。
     */
    public Object poll(long timeoutMs) throws InterruptedException;

    /**
     * 判断队列是否为空（调试/监控用）。
     */
    public boolean isEmpty();

    /** 返回当前队列元素数量（调试/监控用）。 */
    public int size();
}
```

## 核心流程（伪代码）

```
// Fanout 场景（SourceTask → 多个下游节点）：
// TaskPlan 在构造时为每个下游节点各创建一个 DataQueue
// SourceTask 持有 List<DataQueue>，每次 fetch 后逐一 put

DataQueue.put(record):
  queue.put(record)      // ArrayBlockingQueue 保证线程安全，满时阻塞

DataQueue.take():
  return queue.take()    // 空时阻塞

DataQueue.poll(timeoutMs):
  return queue.poll(timeoutMs, TimeUnit.MILLISECONDS)

DataQueue.isEmpty():
  return queue.isEmpty()
```

## 依赖约束

- 仅依赖 JDK `java.util.concurrent.ArrayBlockingQueue`
- 不依赖任何子域组件或 L2 业务定义
- 线程安全（`ArrayBlockingQueue` 内部保证）
