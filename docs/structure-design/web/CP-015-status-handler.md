# L4 — CP-015 StatusHandler

## 组件概述

所属领域：web

职责：处理所有 HTTP 请求（`/`、`/api/status`、`/api/dag`、`/api/logs`）。聚合 `TaskInfo`、`MetricsSnapshot`、`CheckpointInfo` 和 `DagSnapshot` 数据，分别渲染为 HTML 页面或 JSON 响应；读取日志文件末尾行供日志接口返回。所有请求异常仅记录日志，向客户端返回 500，不影响主任务。

## 内部结构

```
StatusHandler（实现 HttpHandler）
 ├── JobGraph jobGraph
 ├── JobExecutor jobExecutor
 ├── MetricsRegistry metricsRegistry
 ├── CheckpointCoordinator checkpointCoordinator
 └── StreamConfig config
```

## 功能小块

### StatusHandler

```java
public class StatusHandler implements HttpHandler {

    public StatusHandler(JobGraph jobGraph,
                         JobExecutor jobExecutor,
                         MetricsRegistry metricsRegistry,
                         CheckpointCoordinator checkpointCoordinator,  // 可为 null（checkpoint 未启用时）
                         StreamConfig config);

    /**
     * 路由分发入口：
     * - path == "/api/status" → handleApiStatus(exchange)
     * - path == "/api/dag"    → handleApiDag(exchange)
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
     *   "nodeMetrics": [
     *     { nodeId, nodeName, type, topologyOrder,
     *       inputCount, outputCount,
     *       rpsHistory: [ { timestampMs, rps }, ... ] },
     *     ...
     *   ]
     * }
     */
    private void handleApiStatus(HttpExchange exchange) throws IOException;

    /**
     * 返回 JSON 格式的 DAG 拓扑数据，Content-Type: application/json。
     * 遍历 jobGraph.nodes（已拓扑排序），构造 DagSnapshot 后序列化。
     * JSON 结构：
     * {
     *   "nodes": [
     *     { "nodeId": "...", "name": "source-1", "type": "SOURCE",
     *       "upstreamIds": [] },
     *     { "nodeId": "...", "name": "map-1",    "type": "FLATMAP",
     *       "upstreamIds": ["..."] },
     *     ...
     *   ]
     * }
     */
    private void handleApiDag(HttpExchange exchange) throws IOException;

    /**
     * 返回日志文件末尾 logTailLines 行内容，Content-Type: text/plain; charset=utf-8。
     * 文件不存在时返回空字符串（HTTP 200）。
     * 读取方式：RandomAccessFile 从末尾向前扫描换行符，取最后 N 行，避免全文加载。
     */
    private void handleApiLogs(HttpExchange exchange) throws IOException;

    /**
     * 返回 HTML 页面，Content-Type: text/html; charset=utf-8。
     * 页面结构（分区）：
     *   ① 任务基本信息区：jobName、status、startTime、配置摘要
     *   ② DAG 拓扑图区：<div id="dag-container"><svg id="dag-svg"></svg></div>
     *      JS 启动后 fetch /api/dag，按节点位置渲染 SVG 方块和连线；
     *      SOURCE 节点绿色，SINK 节点橙色，中间节点蓝色
     *   ③ 节点指标表格区：表头（节点名 | 类型 | 输入总数 | 输出总数 | 近10分钟吞吐趋势）
     *      每行末尾嵌入 <canvas width="200" height="40"> 由 JS 绘制 RPS 折线图
     *   ④ Checkpoint 状态区
     *   ⑤ 日志区：fetch /api/logs，<pre> 展示
     * JS 定时器：每 refreshIntervalSeconds 秒同时 fetch /api/status 和 /api/dag，刷新全部区域。
     * HTML 使用手工字符串拼接，无模板引擎；所有 JS 内嵌于页面，不加载外部资源。
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

    /**
     * 将 DagSnapshot 序列化为 JSON 字符串（私有）。
     * 手工拼接，不依赖第三方 JSON 库。
     */
    private String buildDagJson(DagSnapshot dagSnapshot);

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
  checkpointInfo = (checkpointCoordinator != null)
                   ? checkpointCoordinator.getCheckpointInfo()
                   : CheckpointInfo{ totalCount=0, latestCheckpointId=-1,
                                     latestStatus="NONE", latestStoragePath="",
                                     latestTimestampMs=0 }
  json = buildStatusJson(taskInfo, metrics, checkpointInfo)
  sendResponse(exchange, 200, "application/json", json)

handleApiDag(exchange):
  nodes = []
  for node in jobGraph.nodes:           // jobGraph.nodes 已按拓扑排序
    nodes.add(new DagNodeView(node.nodeId, node.name, node.type, node.upstreamIds))
  dagSnapshot = new DagSnapshot(nodes)
  json = buildDagJson(dagSnapshot)
  sendResponse(exchange, 200, "application/json", json)

handleApiLogs(exchange):
  content = tailFile(config.logFilePath, config.webConfig.logTailLines)
  sendResponse(exchange, 200, "text/plain; charset=utf-8", content)

handleHtml(exchange):
  html = buildHtml()     // 手工拼接完整 HTML，结构见上文功能描述
  sendResponse(exchange, 200, "text/html; charset=utf-8", html)

buildHtml():
  返回包含以下内容的 HTML 字符串：
  1. <style> 简单表格/方块样式，SOURCE 节点 #4CAF50，SINK 节点 #FF9800，中间节点 #2196F3
  2. DAG 区域：<div id="dag-container"><svg id="dag-svg" width="800" height="300"></svg></div>
  3. 指标表格：<table id="metrics-table"><thead>...</thead><tbody id="metrics-body"></tbody></table>
  4. Checkpoint 区域：<div id="cp-section"></div>
  5. 日志区域：<pre id="log-content"></pre>
  6. <script> 内嵌 JS：
     function refresh() {
       fetch('/api/status').then(r => r.json()).then(data => {
         renderTaskInfo(data.taskInfo)
         renderMetrics(data.nodeMetrics)     // 更新表格 + 绘制 RPS canvas
         renderCheckpoint(data.checkpoint)
       })
       fetch('/api/dag').then(r => r.json()).then(data => {
         renderDag(data.nodes)               // SVG 节点方块 + 连线
       })
       fetch('/api/logs').then(r => r.text()).then(text => {
         document.getElementById('log-content').textContent = text
       })
     }
     function renderDag(nodes):
       // 按 nodes 数组顺序布局：每个节点绘制 <rect> + <text>，
       // 根据 upstreamIds 绘制 <line> 连线，SOURCE 绿，SINK 橙，其余蓝
     function renderMetrics(nodeMetrics):
       // 重建 metrics-body 行，每行末尾 <canvas> 调用 drawSparkline(canvas, rpsHistory)
     function drawSparkline(canvas, rpsHistory):
       // Canvas 2D：清空 → 计算 maxRps → 逐点连线绘制折线图
     setInterval(refresh, refreshIntervalSeconds * 1000)
     refresh()                              // 页面加载立即执行一次

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

- 依赖 runtime 子域：`JobExecutor`（只读 TaskInfo）、`JobGraph`（只读，构造 DagSnapshot）
- 依赖 metrics 子域：`MetricsRegistry`（只读 snapshot）
- 依赖 checkpoint 子域：`CheckpointCoordinator`（只读 CheckpointInfo）
- 依赖 L2 定义：`TaskInfo`、`MetricsSnapshot`、`NodeMetricsSummary`、`RpsDataPoint`、
  `CheckpointInfo`、`DagSnapshot`、`DagNodeView`、`StreamConfig`、`WebConfig`、`JobGraph`
- 仅依赖 JDK 标准 IO 和 `com.sun.net.httpserver.HttpHandler`
- 不修改任何运行状态（只读所有依赖）
- HTML 中所有 JS 内嵌，不加载任何外部脚本或 CSS
