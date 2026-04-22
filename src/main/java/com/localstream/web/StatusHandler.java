package com.localstream.web;

import com.localstream.checkpoint.CheckpointCoordinator;
import com.localstream.common.CheckpointInfo;
import com.localstream.common.DagNodeView;
import com.localstream.common.DagSnapshot;
import com.localstream.common.JobGraph;
import com.localstream.common.MetricsSnapshot;
import com.localstream.common.NodeMetricsSummary;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.common.RpsDataPoint;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 处理所有 HTTP 请求（/、/api/status、/api/dag、/api/logs）。
 * 聚合 TaskInfo、MetricsSnapshot、CheckpointInfo 和 DagSnapshot 数据，
 * 渲染为 HTML 页面或 JSON 响应。
 */
public class StatusHandler implements HttpHandler {

    private static final Logger log = Logger.getLogger(StatusHandler.class);

    private final JobGraph jobGraph;
    private final JobExecutor jobExecutor;
    private final MetricsRegistry metricsRegistry;
    private final CheckpointCoordinator checkpointCoordinator; // 可为 null
    private final StreamConfig config;

    public StatusHandler(JobGraph jobGraph,
                         JobExecutor jobExecutor,
                         MetricsRegistry metricsRegistry,
                         CheckpointCoordinator checkpointCoordinator,
                         StreamConfig config) {
        this.jobGraph = jobGraph;
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
            } else if ("/api/dag".equals(path)) {
                handleApiDag(exchange);
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

    // ——— API 处理方法 ———

    private void handleApiStatus(HttpExchange exchange) throws IOException {
        TaskInfo taskInfo = jobExecutor.getTaskInfo();
        MetricsSnapshot metrics = metricsRegistry.snapshot();
        CheckpointInfo cpInfo = resolveCheckpointInfo();
        String json = buildStatusJson(taskInfo, metrics, cpInfo);
        sendResponse(exchange, 200, "application/json; charset=utf-8", json);
    }

    private void handleApiDag(HttpExchange exchange) throws IOException {
        List<DagNodeView> views = new ArrayList<>();
        for (OperatorNode node : jobGraph.nodes) {
            views.add(new DagNodeView(node.nodeId, node.name, node.type,
                    node.upstreamIds != null ? node.upstreamIds : new ArrayList<>()));
        }
        String json = buildDagJson(new DagSnapshot(views));
        sendResponse(exchange, 200, "application/json; charset=utf-8", json);
    }

    private void handleApiLogs(HttpExchange exchange) throws IOException {
        String content = tailFile(config.logFilePath, config.webConfig.logTailLines);
        sendResponse(exchange, 200, "text/plain; charset=utf-8", content);
    }

    private void handleHtml(HttpExchange exchange) throws IOException {
        String html = buildHtml();
        sendResponse(exchange, 200, "text/html; charset=utf-8", html);
    }

    // ——— HTML 构建 ———

    private String buildHtml() {
        int refreshMs = config.webConfig.refreshIntervalSeconds * 1000;
        TaskInfo taskInfo = jobExecutor.getTaskInfo();

        return "<!DOCTYPE html><html><head>"
                + "<meta charset='utf-8'>"
                + "<title>LocalStream - " + escHtml(taskInfo.jobName) + "</title>"
                + buildStyle()
                + buildScript(refreshMs)
                + "</head><body>"
                + "<h2>&#9889; LocalStream Dashboard &mdash; " + escHtml(taskInfo.jobName) + "</h2>"
                + "<div class='section' id='task-section'>"
                +   "<h3>Task Info</h3>"
                +   "<p><b>Status:</b> <span id='task-status' class='tag'>"
                +       escHtml(taskInfo.status) + "</span></p>"
                +   "<p><b>Start Time:</b> <span id='task-start'>N/A</span></p>"
                +   "<p><b>Total Nodes:</b> " + taskInfo.totalNodes + "</p>"
                +   "<p><b>Checkpoint:</b> "
                +       (config.checkpointEnabled
                           ? "enabled, interval=" + config.checkpointIntervalMs + "ms"
                           : "disabled")
                +   "</p>"
                +   "<p><b>Queue Capacity:</b> " + config.queueCapacity + "</p>"
                + "</div>"
                + "<div class='section'>"
                +   "<h3>DAG Topology</h3>"
                +   "<div id='dag-container'><svg id='dag-svg' width='900' height='160'"
                +     " style='background:#1a1a2e;border-radius:6px'></svg></div>"
                + "</div>"
                + "<div class='section'>"
                +   "<h3>Node Metrics (topological order)</h3>"
                +   "<table><thead><tr>"
                +     "<th>Node</th><th>Type</th><th>Input Total</th>"
                +     "<th>Output Total</th><th>RPS (last 10 min)</th>"
                +   "</tr></thead>"
                +   "<tbody id='metrics-body'><tr><td colspan='5'>Loading...</td></tr></tbody>"
                +   "</table>"
                + "</div>"
                + "<div class='section' id='cp-section'>"
                +   "<h3>Checkpoint</h3><p id='cp-content'>Loading...</p>"
                + "</div>"
                + "<div class='section'>"
                +   "<h3>Logs <small>(last " + config.webConfig.logTailLines + " lines)</small>"
                +   " <button onclick='loadLogs()'>Refresh</button></h3>"
                +   "<pre id='log-content' style='max-height:400px;overflow:auto;font-size:12px'>"
                +   "(click Refresh to load)</pre>"
                + "</div>"
                + "<p style='color:#555'>Auto-refresh every " + config.webConfig.refreshIntervalSeconds
                + "s &nbsp;|&nbsp; <a href='/api/status' style='color:#4ec9b0'>Status JSON</a>"
                + " &nbsp;|&nbsp; <a href='/api/dag' style='color:#4ec9b0'>DAG JSON</a></p>"
                + "</body></html>";
    }

    private String buildStyle() {
        return "<style>"
                + "body{font-family:monospace;margin:20px;background:#1e1e1e;color:#d4d4d4}"
                + "h2{color:#4ec9b0}h3{color:#9cdcfe;margin-top:0}"
                + "table{border-collapse:collapse;width:100%}"
                + "th,td{border:1px solid #444;padding:6px 10px;text-align:left;vertical-align:middle}"
                + "th{background:#2d2d2d}"
                + ".section{margin-bottom:20px;padding:14px;background:#252526;border-radius:6px}"
                + ".tag{padding:2px 8px;border-radius:3px;font-weight:bold}"
                + ".RUNNING{background:#0e4f0e;color:#73c991}"
                + ".STOPPED{background:#4f0e0e;color:#f48771}"
                + ".FAILED{background:#4f2200;color:#f9a848}"
                + "button{background:#2d2d2d;color:#d4d4d4;border:1px solid #555;"
                +        "padding:3px 10px;cursor:pointer;border-radius:3px}"
                + "canvas{display:block}"
                + "#dag-container{overflow-x:auto}"
                + "</style>";
    }

    private String buildScript(int refreshMs) {
        return "<script>\n"
                // ——— DAG 渲染 ———
                + "var NODE_W=110,NODE_H=36,GAP_X=70,GAP_Y=50,PAD_X=20,PAD_Y=20;\n"
                + "var COL_SRC='#2e7d32',COL_SINK='#e65100',COL_MID='#1565c0',COL_TXT='#fff';\n"
                + "function renderDag(nodes){\n"
                + "  var svg=document.getElementById('dag-svg');\n"
                + "  svg.innerHTML='';\n"
                + "  if(!nodes||nodes.length===0) return;\n"
                // 计算每个节点的深度（最长路径）
                + "  var depMap={};\n"
                + "  var nodeMap={};\n"
                + "  nodes.forEach(function(n){nodeMap[n.nodeId]=n;});\n"
                + "  function depth(id){\n"
                + "    if(depMap[id]!==undefined) return depMap[id];\n"
                + "    var n=nodeMap[id];\n"
                + "    if(!n||!n.upstreamIds||n.upstreamIds.length===0){depMap[id]=0;return 0;}\n"
                + "    var mx=0;\n"
                + "    n.upstreamIds.forEach(function(uid){mx=Math.max(mx,depth(uid));});\n"
                + "    depMap[id]=mx+1; return mx+1;\n"
                + "  }\n"
                + "  nodes.forEach(function(n){depth(n.nodeId);});\n"
                // 按深度分组，计算坐标
                + "  var groups={};\n"
                + "  nodes.forEach(function(n){\n"
                + "    var d=depMap[n.nodeId];\n"
                + "    if(!groups[d]) groups[d]=[];\n"
                + "    groups[d].push(n);\n"
                + "  });\n"
                + "  var posMap={};\n"
                + "  var maxD=Object.keys(groups).reduce(function(a,b){return Math.max(a,+b);},0);\n"
                + "  var maxInGroup=1;\n"
                + "  Object.keys(groups).forEach(function(d){maxInGroup=Math.max(maxInGroup,groups[d].length);});\n"
                + "  var svgW=PAD_X*2+(maxD+1)*(NODE_W+GAP_X)-GAP_X;\n"
                + "  var svgH=PAD_Y*2+maxInGroup*(NODE_H+GAP_Y)-GAP_Y;\n"
                + "  svg.setAttribute('width',Math.max(svgW,300));\n"
                + "  svg.setAttribute('height',Math.max(svgH,80));\n"
                + "  Object.keys(groups).forEach(function(d){\n"
                + "    var grp=groups[d];\n"
                + "    var totalH=grp.length*(NODE_H+GAP_Y)-GAP_Y;\n"
                + "    var startY=PAD_Y+(Math.max(svgH-PAD_Y*2,totalH)-totalH)/2;\n"
                + "    grp.forEach(function(n,i){\n"
                + "      posMap[n.nodeId]={x:PAD_X+d*(NODE_W+GAP_X),y:startY+i*(NODE_H+GAP_Y)};\n"
                + "    });\n"
                + "  });\n"
                // 先画边
                + "  nodes.forEach(function(n){\n"
                + "    if(!n.upstreamIds) return;\n"
                + "    n.upstreamIds.forEach(function(uid){\n"
                + "      var src=posMap[uid]; var dst=posMap[n.nodeId];\n"
                + "      if(!src||!dst) return;\n"
                + "      var x1=src.x+NODE_W,y1=src.y+NODE_H/2;\n"
                + "      var x2=dst.x,     y2=dst.y+NODE_H/2;\n"
                + "      var mx=(x1+x2)/2;\n"
                + "      var path=document.createElementNS('http://www.w3.org/2000/svg','path');\n"
                + "      path.setAttribute('d','M'+x1+' '+y1+' C'+mx+' '+y1+' '+mx+' '+y2+' '+x2+' '+y2);\n"
                + "      path.setAttribute('stroke','#888');path.setAttribute('stroke-width','2');\n"
                + "      path.setAttribute('fill','none');\n"
                + "      svg.appendChild(path);\n"
                + "      var arr=document.createElementNS('http://www.w3.org/2000/svg','polygon');\n"
                + "      arr.setAttribute('points',(x2)+' '+(y2-5)+' '+(x2+8)+' '+y2+' '+x2+' '+(y2+5));\n"
                + "      arr.setAttribute('fill','#888'); svg.appendChild(arr);\n"
                + "    });\n"
                + "  });\n"
                // 再画节点
                + "  nodes.forEach(function(n){\n"
                + "    var p=posMap[n.nodeId]; if(!p) return;\n"
                + "    var col=n.type==='SOURCE'?COL_SRC:n.type==='SINK'?COL_SINK:COL_MID;\n"
                + "    var rect=document.createElementNS('http://www.w3.org/2000/svg','rect');\n"
                + "    rect.setAttribute('x',p.x);rect.setAttribute('y',p.y);\n"
                + "    rect.setAttribute('width',NODE_W);rect.setAttribute('height',NODE_H);\n"
                + "    rect.setAttribute('rx','5');rect.setAttribute('fill',col);\n"
                + "    svg.appendChild(rect);\n"
                + "    var txt=document.createElementNS('http://www.w3.org/2000/svg','text');\n"
                + "    txt.setAttribute('x',p.x+NODE_W/2);txt.setAttribute('y',p.y+NODE_H/2+5);\n"
                + "    txt.setAttribute('text-anchor','middle');\n"
                + "    txt.setAttribute('fill',COL_TXT);txt.setAttribute('font-size','13');\n"
                + "    txt.setAttribute('font-family','monospace');\n"
                + "    txt.textContent=n.name; svg.appendChild(txt);\n"
                + "  });\n"
                + "}\n"
                // ——— RPS Sparkline ———
                + "function drawSparkline(canvas,history){\n"
                + "  var ctx=canvas.getContext('2d');\n"
                + "  var w=canvas.width,h=canvas.height;\n"
                + "  ctx.clearRect(0,0,w,h);\n"
                + "  if(!history||history.length<2){ctx.fillStyle='#555';ctx.font='10px monospace';"
                + "ctx.fillText('no data',4,h/2+4);return;}\n"
                + "  var maxRps=1;\n"
                + "  history.forEach(function(p){if(p.rps>maxRps)maxRps=p.rps;});\n"
                + "  ctx.strokeStyle='#4ec9b0';ctx.lineWidth=1.5;\n"
                + "  ctx.beginPath();\n"
                + "  history.forEach(function(p,i){\n"
                + "    var x=i*(w-2)/(history.length-1)+1;\n"
                + "    var y=h-2-(p.rps/maxRps)*(h-4);\n"
                + "    if(i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);\n"
                + "  });\n"
                + "  ctx.stroke();\n"
                + "  var last=history[history.length-1].rps;\n"
                + "  ctx.fillStyle='#9cdcfe';ctx.font='9px monospace';\n"
                + "  ctx.fillText(last+'/s',3,10);\n"
                + "}\n"
                // ——— Metrics 表格渲染 ———
                + "function renderMetrics(nodeMetrics){\n"
                + "  var tbody=document.getElementById('metrics-body');\n"
                + "  tbody.innerHTML='';\n"
                + "  if(!nodeMetrics||nodeMetrics.length===0) return;\n"
                + "  nodeMetrics.forEach(function(nm){\n"
                + "    var tr=document.createElement('tr');\n"
                + "    tr.innerHTML='<td>'+nm.nodeName+'</td>'"
                + "               +'<td>'+nm.type+'</td>'"
                + "               +'<td>'+(nm.type==='SOURCE'?'&mdash;':nm.inputCount)+'</td>'"
                + "               +'<td>'+(nm.type==='SINK'?'&mdash;':nm.outputCount)+'</td>';\n"
                + "    var td=document.createElement('td');\n"
                + "    var canvas=document.createElement('canvas');\n"
                + "    canvas.width=200;canvas.height=40;\n"
                + "    canvas.style.background='#1a1a2e';\n"
                + "    td.appendChild(canvas); tr.appendChild(td);\n"
                + "    tbody.appendChild(tr);\n"
                + "    drawSparkline(canvas,nm.rpsHistory);\n"
                + "  });\n"
                + "}\n"
                // ——— Checkpoint 渲染 ———
                + "function renderCheckpoint(cp){\n"
                + "  if(!cp) return;\n"
                + "  var sdf=function(ms){if(!ms||ms<=0)return 'N/A';"
                + "var d=new Date(ms);return d.toLocaleString();};\n"
                + "  document.getElementById('cp-content').innerHTML="
                + "    '<b>Total Completed:</b> '+cp.totalCount+'<br>'"
                + "   +'<b>Latest ID:</b> '+cp.latestCheckpointId+'<br>'"
                + "   +'<b>Status:</b> '+cp.latestStatus+'<br>'"
                + "   +'<b>Path:</b> '+(cp.latestStoragePath||'N/A')+'<br>'"
                + "   +'<b>Time:</b> '+sdf(cp.latestTimestampMs);\n"
                + "}\n"
                // ——— Task Info 渲染 ———
                + "function renderTaskInfo(ti){\n"
                + "  var el=document.getElementById('task-status');\n"
                + "  el.textContent=ti.status; el.className='tag '+ti.status;\n"
                + "  if(ti.startTimeMs>0){\n"
                + "    document.getElementById('task-start').textContent=new Date(ti.startTimeMs).toLocaleString();\n"
                + "  }\n"
                + "}\n"
                // ——— Logs ———
                + "function loadLogs(){\n"
                + "  fetch('/api/logs').then(function(r){return r.text();}).then(function(t){\n"
                + "    document.getElementById('log-content').textContent=t;\n"
                + "  });\n"
                + "}\n"
                // ——— 刷新主循环 ———
                + "function refresh(){\n"
                + "  fetch('/api/status').then(function(r){return r.json();}).then(function(d){\n"
                + "    renderTaskInfo(d.taskInfo);\n"
                + "    renderMetrics(d.nodeMetrics);\n"
                + "    renderCheckpoint(d.checkpoint);\n"
                + "  }).catch(function(){});\n"
                + "  fetch('/api/dag').then(function(r){return r.json();}).then(function(d){\n"
                + "    renderDag(d.nodes);\n"
                + "  }).catch(function(){});\n"
                + "}\n"
                + "setInterval(refresh," + refreshMs + ");\n"
                + "refresh();\n"
                + "</script>\n";
    }

    // ——— JSON 序列化 ———

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
        // nodeMetrics
        sb.append("\"nodeMetrics\":[");
        if (metrics.nodeMetrics != null) {
            for (int i = 0; i < metrics.nodeMetrics.size(); i++) {
                if (i > 0) sb.append(",");
                NodeMetricsSummary nm = metrics.nodeMetrics.get(i);
                sb.append("{");
                sb.append("\"nodeId\":\"").append(escJson(nm.nodeId)).append("\",");
                sb.append("\"nodeName\":\"").append(escJson(nm.nodeName)).append("\",");
                sb.append("\"type\":\"").append(nm.type.name()).append("\",");
                sb.append("\"topologyOrder\":").append(nm.topologyOrder).append(",");
                sb.append("\"inputCount\":").append(nm.inputCount).append(",");
                sb.append("\"outputCount\":").append(nm.outputCount).append(",");
                sb.append("\"rpsHistory\":[");
                if (nm.rpsHistory != null) {
                    for (int j = 0; j < nm.rpsHistory.size(); j++) {
                        if (j > 0) sb.append(",");
                        RpsDataPoint p = nm.rpsHistory.get(j);
                        sb.append("{\"timestampMs\":").append(p.timestampMs)
                          .append(",\"rps\":").append(p.rps).append("}");
                    }
                }
                sb.append("]}");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    private String buildDagJson(DagSnapshot dag) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"nodes\":[");
        for (int i = 0; i < dag.nodes.size(); i++) {
            if (i > 0) sb.append(",");
            DagNodeView n = dag.nodes.get(i);
            sb.append("{");
            sb.append("\"nodeId\":\"").append(escJson(n.nodeId)).append("\",");
            sb.append("\"name\":\"").append(escJson(n.name)).append("\",");
            sb.append("\"type\":\"").append(n.type.name()).append("\",");
            sb.append("\"upstreamIds\":[");
            if (n.upstreamIds != null) {
                for (int j = 0; j < n.upstreamIds.size(); j++) {
                    if (j > 0) sb.append(",");
                    sb.append("\"").append(escJson(n.upstreamIds.get(j))).append("\"");
                }
            }
            sb.append("]}");
        }
        sb.append("]}");
        return sb.toString();
    }

    // ——— 工具方法 ———

    private CheckpointInfo resolveCheckpointInfo() {
        if (checkpointCoordinator != null) {
            return checkpointCoordinator.getCheckpointInfo();
        }
        return new CheckpointInfo();
    }

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
            return "";
        }
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
