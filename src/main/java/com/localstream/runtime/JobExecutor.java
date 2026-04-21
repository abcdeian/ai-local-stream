package com.localstream.runtime;

import com.localstream.common.StreamConfig;
import com.localstream.common.TaskInfo;
import com.localstream.util.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * runtime 子域的线程调度器，职责严格限定在线程生命周期管理范围内。
 * 接收已编译好的 TaskPlan，负责：为每个任务分配独立线程并启动、监控线程异常（fail-fast 策略）、
 * 协调所有线程的有序终止，维护 TaskInfo 供外部查询。
 */
public class JobExecutor {

    private static final Logger log = Logger.getLogger(JobExecutor.class);

    private final TaskPlan plan;
    private final StreamConfig config;
    private final List<Thread> threads = new ArrayList<>();
    private volatile Runnable shutdownHook = null;
    private final TaskInfo taskInfo;

    public JobExecutor(TaskPlan plan, StreamConfig config) {
        this.plan = plan;
        this.config = config;
        this.taskInfo = new TaskInfo(config.jobName, "INITIALIZING",
                plan.getAllTasks().size(), config);
    }

    /**
     * 启动所有任务线程。
     * 为每个任务创建线程，设置 UncaughtExceptionHandler（fail-fast）。
     */
    public void startThreads() {
        for (Runnable task : plan.getAllTasks()) {
            String threadName = resolveThreadName(task);
            Thread t = new Thread(task, threadName);
            t.setUncaughtExceptionHandler((thread, e) -> {
                log.error("Task thread [{}] failed: {}", thread.getName(), e.getMessage());
                taskInfo.status = "FAILED";
                Runnable hook = shutdownHook;
                if (hook != null) hook.run();
            });
            threads.add(t);
            t.start();
        }
        taskInfo.status = "RUNNING";
        taskInfo.startTimeMs = System.currentTimeMillis();
        log.info("JobExecutor: all {} task threads started, job [{}] is RUNNING",
                threads.size(), config.jobName);
    }

    /**
     * 阻塞调用线程直到所有任务线程结束（Thread.join）。
     * 用于 StreamEnv 主线程等待。
     */
    public void awaitTermination() throws InterruptedException {
        for (Thread t : threads) {
            t.join();
        }
    }

    /**
     * 终止所有任务线程（interrupt all threads）。
     * 由 shutdownHook 或 StreamEnv 清理阶段调用。幂等。
     */
    public void stop() {
        log.info("JobExecutor: stopping all task threads...");
        for (Thread t : threads) {
            t.interrupt();
        }
        if (!"FAILED".equals(taskInfo.status)) {
            taskInfo.status = "STOPPED";
        }
    }

    /**
     * 注入全局 shutdown 钩子（由 StreamEnv 在装配阶段调用）。
     * 线程异常时触发此钩子，协调 Checkpoint/Web/任务线程的统一停止。
     */
    public void setShutdownHook(Runnable shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    /** 返回 TaskInfo 供 web 子域查询（只读） */
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    private String resolveThreadName(Runnable task) {
        if (task instanceof SourceTask) return "task-" + ((SourceTask) task).getNodeId();
        if (task instanceof ProcessorTask) return "task-" + ((ProcessorTask) task).getNodeId();
        if (task instanceof SinkTask) return "task-" + ((SinkTask) task).getNodeId();
        return "task-unknown";
    }
}
