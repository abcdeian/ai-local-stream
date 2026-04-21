package com.localstream.common;

/** Checkpoint 运行状态摘要，由 checkpoint 子域产出，供 web 子域查询展示。 */
public class CheckpointInfo {
    public long totalCount = 0;
    public long latestCheckpointId = -1;
    public String latestStatus = "NONE";     // "SUCCESS" / "FAILED" / "NONE"
    public String latestStoragePath = "";
    public long latestTimestampMs = 0;
}
