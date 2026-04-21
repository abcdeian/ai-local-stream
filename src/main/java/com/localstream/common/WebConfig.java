package com.localstream.common;

/** Web 服务的配置，作为 StreamConfig 的组成部分。 */
public class WebConfig {
    /** Web 服务监听端口，默认 8080 */
    public int port = 8080;
    /** 是否启用 Web 服务，默认 true */
    public boolean enabled = true;
    /** 前端页面数据自动刷新间隔（秒），默认 5 */
    public int refreshIntervalSeconds = 5;
    /** Web 日志页面最大展示行数，默认 500 */
    public int logTailLines = 500;
}
