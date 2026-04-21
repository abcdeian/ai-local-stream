# L4 — CP-015 StatusHandler

## 组件概述

所属领域：web

职责：处理所有 HTTP 请求（`/`、`/api/status`、`/api/logs`）。聚合 `TaskInfo`、`MetricsSnapshot`、`CheckpointInfo` 数据，分别渲染为 HTML 页面或 JSON 响应；读取日志文件末尾行供日志接口返回。所有请求异常仅记录日志，向客户端返回 500，不影响主任务。

## 内部结构

```
StatusHandler（实现 HttpHandler）
 ├── JobExecutor jobExecutor
 ├── MetricsRegistry metricsRegistry
 ├── CheckpointCoordinator checkpointCoordinator
 └── StreamConfig config
```

## 功能小块

### StatusHandler

```java
public class StatusHandler implements HttpHandler {

    public StatusHandler(JobExecutor jobExecutor,
                         MetricsRegistry metricsRegistry,
                         CheckpointCoordinator checkpointCoordinator,  // 可为 null（checkpoint 未启用时）
                         StreamConfig config);

    /**
     * 路由分发入口：
     * - path == "/api/status" → handleApiStatus(exchange)
     * - path == "/api/logs"   → handleApiLogs(exchange)
     * - 其他（"/"）           → handleHtml(exchange)
     * 任何异常捕获后返回 HTTP 500，记录 error 日志。
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException;

    /**
     * 返回 JSON 格式的聚合状态数据，Content-Type: application/json。
     * JSON 结构：
     * {
     *   "taskInfo": { jobName, status, startTimeMs, totalNodes,
     *                 config: { checkpointEnabled, checkpointIntervalMs,
     *                           checkpointDir, queueCapacity, webPort } },
     *   "checkpoint": { totalCount, latestCheckpointId, latestStatus,
     *                   latestStoragePath, latestTimestampMs },
     *   "metrics": [ { nodeId, nodeName, metricName, metricValue }, ... ]
     * }
     */
    private void handleApiStatus(HttpExchange exchange) throws IOException;

    /**
     * 返回日志文件末尾 logTailLines 行内容，Content-Type: text/plain; charset=utf-8。
     * 文件不存在时返回空字符串（HTTP 200）。
     * 读取方式：RandomAccessFile 从末尾向前扫描换行符，取最后 N 行，避免全文加载。
     */
    private void handleApiLogs(HttpExchange exchange) throws IOException;

    /**
     * 返回 HTML 页面，Content-Type: text/html; charset=utf-8。
     * 页面结构：
     * - 顶部展示 taskInfo（任务名、状态、启动时间、配置）
     * - 中部展示 metrics（表格：nodeId、nodeName、指标名、值）
     * - 中部展示 checkpoint（总次数、最近状态、路径、时间）
     * - 底部展示日志区（iframe 或 fetch /api/logs）
     * - JS 定时轮询 /api/status（每 refreshIntervalSeconds 刷新数据）
     * HTML 使用手工字符串拼接，无模板引擎。
     */
    private void handleHtml(HttpExchange exchange) throws IOException;

    /**
     * 读取文件末尾 maxLines 行（私有工具方法）。
     * 使用 RandomAccessFile 从文件末尾向前扫描 '\n'，
     * 累积行直到达到 maxLines 或到达文件头，返回字符串。
     */
    private String tailFile(String filePath, int maxLines) throws IOException;

    /**
     * 将 TaskInfo + MetricsSnapshot + CheckpointInfo 序列化为 JSON 字符串（私有）。
     * 手工拼接，不依赖第三方 JSON 库。
     */
    private String buildStatusJson(TaskInfo taskInfo,
                                   MetricsSnapshot metrics,
                                   CheckpointInfo checkpointInfo);

    /** 发送 HTTP 响应（私有工具方法） */
    private void sendResponse(HttpExchange exchange, int statusCode,
                              String contentType, String body) throws IOException;
}
```

## 核心流程（伪代码）

```
handleApiStatus(exchange):
  taskInfo      = jobExecutor.getTaskInfo()
  metrics       = metricsRegistry.snapshot()
  // checkpointCoordinator 可能为 null（checkpoint 未启用时），返回空占位值
  checkpointInfo = (checkpointCoordinator != null)
                   ? checkpointCoordinator.getCheckpointInfo()
                   : CheckpointInfo{ totalCount=0, latestCheckpointId=-1,
                                     latestStatus="NONE", latestStoragePath="",
                                     latestTimestampMs=0 }
  json = buildStatusJson(taskInfo, metrics, checkpointInfo)
  sendResponse(exchange, 200, "application/json", json)

handleApiLogs(exchange):
  content = tailFile(config.logFilePath, config.webConfig.logTailLines)
  sendResponse(exchange, 200, "text/plain; charset=utf-8", content)

handleHtml(exchange):
  html = "<html>..."         // 手工拼接完整 HTML，内嵌 JS 定时 fetch /api/status
  sendResponse(exchange, 200, "text/html; charset=utf-8", html)

tailFile(filePath, maxLines):
  file = new RandomAccessFile(filePath, "r")
  pos = file.length() - 1
  linesFound = 0
  while pos > 0 and linesFound <= maxLines:
    file.seek(pos)
    if file.readByte() == '\n': linesFound++
    pos--
  // 读取 pos ~ end 区间的文本，返回
```

## 依赖约束

- 依赖 runtime 子域：`JobExecutor`（只读 TaskInfo）
- 依赖 metrics 子域：`MetricsRegistry`（只读 snapshot）
- 依赖 checkpoint 子域：`CheckpointCoordinator`（只读 CheckpointInfo）
- 依赖 L2 定义：`TaskInfo`、`MetricsSnapshot`、`MetricEntry`、`CheckpointInfo`、`StreamConfig`、`WebConfig`
- 仅依赖 JDK 标准 IO 和 `com.sun.net.httpserver.HttpHandler`
- 不修改任何运行状态（只读所有依赖）
