package com.localstream.common;

/** 用户在创建 StreamEnv 时可选择性地传入配置，控制系统行为。 */
public class StreamConfig {
    /** 任务名称，默认 "LocalStreamJob" */
    public String jobName = "LocalStreamJob";
    /** Checkpoint 触发周期（毫秒），默认 60000（1分钟） */
    public long checkpointIntervalMs = 60_000L;
    /** Checkpoint 存储根路径，默认 "./checkpoints" */
    public String checkpointDir = "./checkpoints";
    /** 节点间队列容量，默认 1024 */
    public int queueCapacity = 1024;
    /** 是否启用 Checkpoint，默认 true */
    public boolean checkpointEnabled = true;
    /** Checkpoint ack 等待超时时间（毫秒），默认 30000（30秒） */
    public long checkpointAckTimeoutMs = 30_000L;
    /** 日志文件路径，默认 "./logs/localstream.log" */
    public String logFilePath = "./logs/localstream.log";
    /** Web 服务配置 */
    public WebConfig webConfig = new WebConfig();
}
