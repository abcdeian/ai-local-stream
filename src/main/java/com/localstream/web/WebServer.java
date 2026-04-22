package com.localstream.web;

import com.localstream.checkpoint.CheckpointCoordinator;
import com.localstream.common.JobGraph;
import com.localstream.common.StreamConfig;
import com.localstream.metrics.MetricsRegistry;
import com.localstream.runtime.JobExecutor;
import com.localstream.util.Logger;
import com.sun.net.httpserver.HttpServer;

import java.net.BindException;
import java.net.InetSocketAddress;

/**
 * 管理 JDK 内置 HttpServer 的生命周期（启动、停止），注册路由与对应 Handler。
 * 端口被占用时降级（打印警告，web 不可用，不影响主任务）。
 */
public class WebServer {

    private static final Logger log = Logger.getLogger(WebServer.class);

    private final StreamConfig config;
    private final JobGraph jobGraph;
    private final JobExecutor jobExecutor;
    private final MetricsRegistry metricsRegistry;
    private final CheckpointCoordinator checkpointCoordinator;
    private HttpServer httpServer;
    private boolean available = false;

    public WebServer(StreamConfig config,
                     JobGraph jobGraph,
                     JobExecutor jobExecutor,
                     MetricsRegistry metricsRegistry,
                     CheckpointCoordinator checkpointCoordinator) {
        this.config = config;
        this.jobGraph = jobGraph;
        this.jobExecutor = jobExecutor;
        this.metricsRegistry = metricsRegistry;
        this.checkpointCoordinator = checkpointCoordinator;
    }

    /**
     * 启动 Web 服务：绑定端口，注册路由，后台启动。
     * 端口占用时捕获 BindException，打印 warn 日志，available=false，不抛异常。
     */
    public void start() {
        try {
            StatusHandler statusHandler = new StatusHandler(
                    jobGraph, jobExecutor, metricsRegistry, checkpointCoordinator, config);

            httpServer = HttpServer.create(
                    new InetSocketAddress(config.webConfig.port), 0);
            httpServer.createContext("/",           statusHandler);
            httpServer.createContext("/api/status", statusHandler);
            httpServer.createContext("/api/dag",    statusHandler);
            httpServer.createContext("/api/logs",   statusHandler);
            httpServer.setExecutor(null);
            httpServer.start();
            available = true;
            log.info("Web server started at http://localhost:{}", config.webConfig.port);
        } catch (BindException e) {
            available = false;
            log.warn("Web server failed to start, port {} is in use", config.webConfig.port);
        } catch (Exception e) {
            available = false;
            log.warn("Web server failed to start: {}", e.getMessage());
        }
    }

    /**
     * 停止 Web 服务，释放端口。
     * 若 available=false，直接返回。
     */
    public void stop() {
        if (available && httpServer != null) {
            httpServer.stop(0);
            log.info("Web server stopped");
        }
    }
}
