package com.localstream.common;

/** 单个 RPS 历史采样点，记录某一时刻的每秒处理速率。 */
public class RpsDataPoint {
    public final long timestampMs;  // 采样时的 Unix 毫秒时间戳
    public final long rps;          // 该时刻的每秒处理条数（整数近似值）

    public RpsDataPoint(long timestampMs, long rps) {
        this.timestampMs = timestampMs;
        this.rps = rps;
    }
}
