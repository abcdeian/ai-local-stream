# L4 — CP-014 WebServer

## 组件概述

所属领域：web

职责：管理 JDK 内置 `HttpServer` 的生命周期（启动、停止），注册路由与对应 Handler。端口被占用时降级（打印警告，web 不可用，不影响主任务）。

## 内部结构

```
WebServer
 ├── HttpServer httpServer           // JDK com.sun.net.httpserver.HttpServer
 ├── StreamConfig config
 ├── JobGraph jobGraph
 ├── StatusHandler statusHandler
 └── boolean available               // 端口绑定是否成功
```

## 功能小块

### WebServer

```java
public class WebServer {

    public WebServer(StreamConfig config,
                     JobGraph jobGraph,
                     JobExecutor jobExecutor,
                     MetricsRegistry metricsRegistry,
                     CheckpointCoordinator checkpointCoordinator);

    /**
     * 启动 Web 服务：
     * 1. 创建 StatusHandler，注入 jobGraph/jobExecutor/metricsRegistry/checkpointCoordinator 引用
     * 2. 绑定端口 config.webConfig.port，创建 HttpServer
     * 3. 注册路由：
     *    - GET /           → statusHandler（HTML）
     *    - GET /api/status → statusHandler（JSON）
     *    - GET /api/dag    → statusHandler（DAG JSON）
     *    - GET /api/logs   → statusHandler（日志文本）
     * 4. 启动 HttpServer（后台线程，不阻塞主流程）
     * 端口占用时捕获 BindException，打印 warn 日志，available=false，不抛异常。
     */
    public void start();

    /**
     * 停止 Web 服务，释放端口（由 JobExecutor.stop() 触发的 shutdown 钩子调用）。
     * 若 available=false，直接返回。
     */
    public void stop();
}
```

## 核心流程（伪代码）

```
WebServer.start():
  statusHandler = new StatusHandler(jobGraph, jobExecutor, metricsRegistry,
                                    checkpointCoordinator, config)
  try:
    httpServer = HttpServer.create(
        new InetSocketAddress(config.webConfig.port), 0)
    httpServer.createContext("/",           statusHandler)
    httpServer.createContext("/api/status", statusHandler)
    httpServer.createContext("/api/dag",    statusHandler)
    httpServer.createContext("/api/logs",   statusHandler)
    httpServer.setExecutor(null)            // 使用默认线程池
    httpServer.start()
    available = true
    log.info("Web server started at http://localhost:{}", config.webConfig.port)
  catch BindException e:
    available = false
    log.warn("Web server failed to start, port {} is in use", config.webConfig.port)

WebServer.stop():
  if available and httpServer != null:
    httpServer.stop(0)
```

## 依赖约束

- 仅依赖 JDK 内置 `com.sun.net.httpserver.HttpServer`，不引入任何 Web 框架
- 依赖同域组件：`StatusHandler`
- 依赖 runtime 子域：`JobExecutor`（只读引用）
- 依赖 metrics 子域：`MetricsRegistry`（只读引用）
- 依赖 checkpoint 子域：`CheckpointCoordinator`（只读引用）
- 依赖 L2 定义：`StreamConfig`、`WebConfig`、`JobGraph`
