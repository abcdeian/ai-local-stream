package com.localstream.api;

import com.localstream.checkpoint.CheckpointCoordinator;
import com.localstream.checkpoint.CheckpointScheduler;
import com.localstream.common.DagValidationException;
import com.localstream.common.JobGraph;
import com.localstream.common.OperatorNode;
import com.localstream.common.OperatorType;
import com.localstream.common.StreamConfig;
import com.localstream.dag.DagBuilder;
import com.localstream.dag.DagValidator;
import com.localstream.metrics.MetricsRegistry;
import com.localstream.optimizer.DagOptimizer;
import com.localstream.runtime.JobExecutor;
import com.localstream.runtime.TaskPlan;
import com.localstream.state.LocalDiskStateBackend;
import com.localstream.util.Logger;
import com.localstream.web.WebServer;

/**
 * 用户编写流水线的唯一入口类，同时承担整体组件装配者（Assembler）的角色。
 * 持有 StreamConfig 和 DAG 构建上下文（DagBuilder），提供 addSource() 注册数据来源，
 * 提供 start() 触发校验、完成所有子域组件的创建与装配、启动任务并阻塞主线程直到结束。
 */
public class StreamEnv {

    private static final Logger log = Logger.getLogger(StreamEnv.class);

    private final StreamConfig config;
    private final DagBuilder dagBuilder;

    /** 使用默认配置创建 */
    public StreamEnv() {
        this(new StreamConfig());
    }

    /** 使用自定义配置创建 */
    public StreamEnv(StreamConfig config) {
        this.config = config;
        this.dagBuilder = new DagBuilder();
    }

    /**
     * 注册一个 Source 节点，返回代表该节点输出流的 DataSet。
     */
    public <T> DataSet<T> addSource(com.localstream.common.SourceFunction<T> sourceFunction) {
        OperatorNode node = new OperatorNode(OperatorType.SOURCE, sourceFunction,
                java.util.Collections.emptyList());
        dagBuilder.addNode(node);
        return new DataSet<>(node, this);
    }

    /**
     * 触发整个流水线的校验、装配与启动。
     * start() 成功后当前线程阻塞，直到任务结束。
     */
    public void start() throws Exception {
        // 初始化日志文件
        Logger.init(config.logFilePath);
        log.info("========== Starting LocalStream job: {} ==========", config.jobName);

        // 1. DAG 构建与校验
        JobGraph graph = dagBuilder.build();
        new DagValidator().validate(graph);
        log.info("DAG validated: {} nodes", graph.nodes.size());

        // 1b. DAG 优化：移除死节点
        new DagOptimizer().optimize(graph);
        log.info("DAG optimized: {} nodes after dead-node elimination", graph.nodes.size());

        // 2. 创建 MetricsRegistry，注册所有节点（按拓扑顺序）
        MetricsRegistry metricsRegistry = new MetricsRegistry();
        for (OperatorNode node : graph.nodes) {
            metricsRegistry.register(node);
        }

        // 3. 编译任务网络
        TaskPlan plan = new TaskPlan(graph, config, metricsRegistry);

        // 3b. 创建 JobExecutor
        JobExecutor executor = new JobExecutor(plan, config);

        // 4. Checkpoint 相关装配（如启用）
        CheckpointCoordinator coordinator = null;
        CheckpointScheduler scheduler = null;
        if (config.checkpointEnabled) {
            LocalDiskStateBackend stateBackend = new LocalDiskStateBackend(config.checkpointDir);
            coordinator = new CheckpointCoordinator(
                    plan.getSourceTasks(),
                    plan.getNonSourceNodeCount(),
                    plan.getCheckpointables(),
                    stateBackend,
                    config.checkpointAckTimeoutMs);

            // 4a. 将 coordinator 作为 CheckpointAckListener 注入到所有 ProcessorTask/SinkTask
            plan.setAckListener(coordinator);

            // 4b. 若存在历史 Checkpoint，执行状态恢复（失败则抛异常，拒绝启动）
            long restoreId = stateBackend.latestCheckpointId();
            if (restoreId != -1) {
                log.info("Found latest checkpoint id={}, restoring...", restoreId);
                coordinator.restore(restoreId);
            }

            scheduler = new CheckpointScheduler(config.checkpointIntervalMs, coordinator);
        }

        // 5. 启动 Web 服务（如启用）
        final CheckpointCoordinator finalCoordinator = coordinator;
        WebServer webServer = null;
        if (config.webConfig.enabled) {
            webServer = new WebServer(config, graph, executor, metricsRegistry, finalCoordinator);
            webServer.start();
        }

        // 6. 注册全局 shutdown 钩子
        final CheckpointScheduler finalScheduler = scheduler;
        final WebServer finalWebServer = webServer;
        final JobExecutor finalExecutor = executor;
        final MetricsRegistry finalMetrics = metricsRegistry;
        Runnable shutdown = () -> {
            finalExecutor.stop();
            if (finalScheduler != null) finalScheduler.stop();
            if (finalWebServer != null) finalWebServer.stop();
            finalMetrics.stop();
        };
        executor.setShutdownHook(shutdown);

        // 7. 启动所有任务线程
        executor.startThreads();

        // 8. 启动 Checkpoint 调度 + MetricsRegistry 采样（线程就绪后再开始）
        if (scheduler != null) scheduler.start();
        metricsRegistry.start();

        // 9. 阻塞直到所有任务线程结束
        log.info("Job [{}] running. Web dashboard: http://localhost:{}",
                config.jobName, config.webConfig.port);
        executor.awaitTermination();

        // 10. 清理
        shutdown.run();
        log.info("========== Job [{}] finished ==========", config.jobName);
    }

    /** 获取当前配置（供 DataSet 内部使用） */
    StreamConfig getConfig() {
        return config;
    }

    /** 获取 DagBuilder（供 DataSet 内部注册节点使用） */
    DagBuilder getDagBuilder() {
        return dagBuilder;
    }
}
