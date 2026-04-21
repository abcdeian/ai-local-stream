package com.localstream.runtime;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 有界阻塞队列包装，连接 DAG 中相邻节点的数据通道。
 * 元素类型为 Object，可同时承载普通数据记录和 CheckpointBarrier。
 * 写满时写入方阻塞（天然背压），读空时读取方阻塞。
 */
public class DataQueue {

    private final ArrayBlockingQueue<Object> queue;
    private final String queueId; // 关联的下游节点 nodeId（调试用）

    public DataQueue(int capacity, String queueId) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.queueId = queueId;
    }

    /**
     * 向队列写入一条数据。队列满时阻塞（背压）。
     */
    public void put(Object record) throws InterruptedException {
        queue.put(record);
    }

    /**
     * 从队列读取一条数据，队列空时阻塞。
     */
    public Object take() throws InterruptedException {
        return queue.take();
    }

    /**
     * 尝试读取一条数据，超过 timeoutMs 毫秒无数据则返回 null。
     * 用于 UNION 节点轮询消费，避免单队列长期阻塞。
     */
    public Object poll(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public String getQueueId() {
        return queueId;
    }
}
