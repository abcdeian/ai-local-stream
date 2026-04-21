package com.localstream.common;

/** 任务运行时信息，由 runtime 子域产出，供 web 子域查询展示。 */
public class TaskInfo {
    public String jobName;
    public long startTimeMs;
    public String status;       // INITIALIZING / RUNNING / STOPPED / FAILED
    public int totalNodes;
    public StreamConfig config;

    public TaskInfo(String jobName, String status, int totalNodes, StreamConfig config) {
        this.jobName = jobName;
        this.status = status;
        this.totalNodes = totalNodes;
        this.config = config;
        this.startTimeMs = 0L;
    }
}
