package com.localstream.web;

import com.localstream.checkpoint.CheckpointCoordinator;
import com.localstream.common.CheckpointInfo;
import com.localstream.common.MetricEntry;
import com.localstream.common.MetricsSnapshot;
import com.localstream.common.StreamConfig;
import com.localstream.common.TaskInfo;
import com.localstream.metrics.MetricsRegistry;
import com.localstream.runtime.JobExecutor;
import com.localstream.util.Logger;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 处理所有 HTTP 请求（/、/api/status、/api/logs）。
 * 聚合 TaskInfo、MetricsSnapshot、CheckpointInfo 数据，渲染为 HTML 或 JSON 响应。
 */
public class StatusHandler implements HttpHandler {

    private static final Logger log = Logger.getLogger(StatusHandler.class);

    private final JobExecutor jobExecutor;
    private final MetricsRegistry metricsRegistry;
    private final CheckpointCoordinator checkpointCoordinator; // 可为 null（checkpoint 未启用时）
    private final StreamConfig config;

    public StatusHandler(JobExecutor jobExecutor,
                         MetricsRegistry metricsRegistry,
                         CheckpointCoordinator checkpointCoordinator,
                         StreamConfig config) {
        this.jobExecutor = jobExecutor;
        this.metricsRegistry = metricsRegistry;
        this.checkpointCoordinator = checkpointCoordinator;
        this.config = config;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            if ("/api/status".equals(path)) {
                handleApiStatus(exchange);
            } else if ("/api/logs".equals(path)) {
                handleApiLogs(exchange);
            } else {
                handleHtml(exchange);
            }
        } catch (Exception e) {
            log.error("HTTP handler error: {}", e.getMessage());
            sendResponse(exchange, 500, "text/plain", "Internal Server Error: " + e.getMessage());
        }
    }

    private void handleApiStatus(HttpExchange exchange) throws IOException {
        TaskInfo taskInfo = jobExecutor.getTaskInfo();
        MetricsSnapshot metrics = metricsRegistry.snapshot();
        CheckpointInfo cpInfo = (checkpointCoordinator != null)
                ? checkpointCoordinator.getCheckpointInfo()
                : new CheckpointInfo();
        String json = buildStatusJson(taskInfo, metrics, cpInfo);
        sendResponse(exchange, 200, "application/json; charset=utf-8", json);
    }

    private void handleApiLogs(HttpExchange exchange) throws IOException {
        String content = tailFile(config.logFilePath, config.webConfig.logTailLines);
        sendResponse(exchange, 200, "text/plain; charset=utf-8", content);
    }

    private void handleHtml(HttpExchange exchange) throws IOException {
        TaskInfo taskInfo = jobExecutor.getTaskInfo();
        MetricsSnapshot metrics = metricsRegistry.snapshot();
        CheckpointInfo cpInfo = (checkpointCoordinator != null)
                ? checkpointCoordinator.getCheckpointInfo()
                : new CheckpointInfo();

        int refreshSec = config.webConfig.refreshIntervalSeconds;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startTime = taskInfo.startTimeMs > 0
                ? sdf.format(new Date(taskInfo.startTimeMs)) : "N/A";
        String cpTime = cpInfo.latestTimestampMs > 0
                ? sdf.format(new Date(cpInfo.latestTimestampMs)) : "N/A";

        StringBuilder metricsRows = new StringBuilder();
        if (metrics.entries != null) {
            for (MetricEntry e : metrics.entries) {
                metricsRows.append(String.format(
                        "<tr><td>%s</td><td>%s</td><td>%s</td><td>%d</td></tr>",
                        escHtml(e.nodeId), escHtml(e.nodeName),
                        escHtml(e.metricName), e.metricValue));
            }
        }

        String html = "<!DOCTYPE html><html><head>"
                + "<meta charset='utf-8'><title>LocalStream - " + escHtml(taskInfo.jobName) + "</title>"
                + "<style>body{font-family:monospace;margin:20px;background:#1e1e1e;color:#d4d4d4}"
                + "h2{color:#4ec9b0}table{border-collapse:collapse;width:100%}"
                + "th,td{border:1px solid #555;padding:6px 10px;text-align:left}"
                + "th{background:#2d2d2d}.section{margin-bottom:20px;padding:10px;background:#252526;border-radius:4px}"
                + ".tag{padding:2px 8px;border-radius:3px;font-weight:bold}"
                + ".run{background:#0e4f0e;color:#73c991}.stop{background:#4f0e0e;color:#f48771}"
                + ".fail{background:#4f2200;color:#f9a848}</style>"
                + "<script>function refresh(){fetch('/api/status').then(r=>r.json()).then(d=>{"
                + "document.getElementById('status').textContent=d.taskInfo.status;"
                + "}).catch(()=>{})} setInterval(refresh," + (refreshSec * 1000) + ");</script>"
                + "</head><body>"
                + "<h2>LocalStream Dashboard</h2>"
                + "<div class='section'><h3>Task Info</h3>"
                + "<p><b>Job Name:</b> " + escHtml(taskInfo.jobName) + "</p>"
                + "<p><b>Status:</b> <span id='status' class='tag run'>" + escHtml(taskInfo.status) + "</span></p>"
                + "<p><b>Start Time:</b> " + startTime + "</p>"
                + "<p><b>Total Nodes:</b> " + taskInfo.totalNodes + "</p>"
                + "<p><b>Checkpoint:</b> " + (config.checkpointEnabled ? "enabled, interval=" + config.checkpointIntervalMs + "ms" : "disabled") + "</p>"
                + "<p><b>Queue Capacity:</b> " + config.queueCapacity + "</p></div>"
                + "<div class='section'><h3>Metrics</h3>"
                + "<table><tr><th>NodeId</th><th>NodeName</th><th>Metric</th><th>Value</th></tr>"
                + metricsRows.toString() + "</table></div>"
                + "<div class='section'><h3>Checkpoint</h3>"
                + "<p><b>Total Completed:</b> " + cpInfo.totalCount + "</p>"
                + "<p><b>Latest ID:</b> " + cpInfo.latestCheckpointId + "</p>"
                + "<p><b>Status:</b> " + escHtml(cpInfo.latestStatus) + "</p>"
                + "<p><b>Path:</b> " + escHtml(cpInfo.latestStoragePath) + "</p>"
                + "<p><b>Time:</b> " + cpTime + "</p></div>"
                + "<div class='section'><h3>Logs <small>(last " + config.webConfig.logTailLines + " lines)</small>"
                + " <button onclick=\"fetch('/api/logs').then(r=>r.text()).then(t=>{document.getElementById('logs').textContent=t})\">Refresh</button></h3>"
                + "<pre id='logs' style='max-height:400px;overflow:auto;font-size:12px'>(click Refresh to load logs)</pre></div>"
                + "<p style='color:#555'>Auto-refresh every " + refreshSec + "s &nbsp;|&nbsp; "
                + "<a href='/api/status' style='color:#4ec9b0'>JSON API</a></p>"
                + "</body></html>";

        sendResponse(exchange, 200, "text/html; charset=utf-8", html);
    }

    /**
     * 读取文件末尾 maxLines 行。
     * 使用 RandomAccessFile 从末尾向前扫描换行符，避免全文加载。
     */
    private String tailFile(String filePath, int maxLines) {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long length = file.length();
            if (length == 0) return "";
            long pos = length - 1;
            int linesFound = 0;
            while (pos > 0 && linesFound <= maxLines) {
                file.seek(pos);
                if (file.readByte() == '\n') linesFound++;
                pos--;
            }
            file.seek(pos + 2);
            byte[] bytes = new byte[(int) (length - pos - 2)];
            file.readFully(bytes);
            return new String(bytes, "UTF-8");
        } catch (Exception e) {
            return ""; // 文件不存在或读取失败，返回空字符串
        }
    }

    /** 构建 JSON 格式的聚合状态数据（手工拼接，不依赖第三方 JSON 库） */
    private String buildStatusJson(TaskInfo taskInfo, MetricsSnapshot metrics, CheckpointInfo cpInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        // taskInfo
        sb.append("\"taskInfo\":{");
        sb.append("\"jobName\":\"").append(escJson(taskInfo.jobName)).append("\",");
        sb.append("\"status\":\"").append(escJson(taskInfo.status)).append("\",");
        sb.append("\"startTimeMs\":").append(taskInfo.startTimeMs).append(",");
        sb.append("\"totalNodes\":").append(taskInfo.totalNodes).append(",");
        sb.append("\"config\":{");
        sb.append("\"checkpointEnabled\":").append(config.checkpointEnabled).append(",");
        sb.append("\"checkpointIntervalMs\":").append(config.checkpointIntervalMs).append(",");
        sb.append("\"checkpointDir\":\"").append(escJson(config.checkpointDir)).append("\",");
        sb.append("\"queueCapacity\":").append(config.queueCapacity).append(",");
        sb.append("\"webPort\":").append(config.webConfig.port);
        sb.append("}},");
        // checkpoint
        sb.append("\"checkpoint\":{");
        sb.append("\"totalCount\":").append(cpInfo.totalCount).append(",");
        sb.append("\"latestCheckpointId\":").append(cpInfo.latestCheckpointId).append(",");
        sb.append("\"latestStatus\":\"").append(escJson(cpInfo.latestStatus)).append("\",");
        sb.append("\"latestStoragePath\":\"").append(escJson(cpInfo.latestStoragePath)).append("\",");
        sb.append("\"latestTimestampMs\":").append(cpInfo.latestTimestampMs);
        sb.append("},");
        // metrics
        sb.append("\"metrics\":[");
        if (metrics.entries != null) {
            for (int i = 0; i < metrics.entries.size(); i++) {
                MetricEntry e = metrics.entries.get(i);
                if (i > 0) sb.append(",");
                sb.append("{\"nodeId\":\"").append(escJson(e.nodeId)).append("\",");
                sb.append("\"nodeName\":\"").append(escJson(e.nodeName)).append("\",");
                sb.append("\"metricName\":\"").append(escJson(e.metricName)).append("\",");
                sb.append("\"metricValue\":").append(e.metricValue).append("}");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    private void sendResponse(HttpExchange exchange, int code, String contentType, String body)
            throws IOException {
        byte[] bytes = body.getBytes("UTF-8");
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String escHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    private static String escJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r");
    }
}
