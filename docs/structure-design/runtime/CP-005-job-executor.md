# L4 — CP-005 JobExecutor

## 组件概述

所属领域：runtime

职责：runtime 子域的线程调度器，职责严格限定在线程生命周期管理范围内。
接收已编译好的 `TaskPlan`，负责：为每个任务分配独立线程并启动、监控线程异常（fail-fast 策略）、协调所有线程的有序终止，维护 `TaskInfo` 供外部查询。

不负责任务创建、Checkpoint 配置、Web 服务启动——这些由 `StreamEnv` 统一装配。

## 内部结构

```
JobExecutor
 ├── TaskPlan plan                    // 持有所有任务实例（由 StreamEnv 传入）
 ├── StreamConfig config
 ├── List<Thread> threads             // 所有任务线程
 ├── Runnable shutdownHook            // 由 StreamEnv 注入的全局 shutdown 逻辑
 └── TaskInfo taskInfo                // 持有运行状态，供 Web 查询
```

## 功能小块

### JobExecutor

```java
public class JobExecutor {

    public JobExecutor(TaskPlan plan, StreamConfig config);

    /**
     * 启动所有任务线程。
     * 为每个任务创建线程，设置 UncaughtExceptionHandler（触发 shutdownHook，fail-fast）。
     * 所有线程启动后更新 TaskInfo.status = RUNNING，记录 startTimeMs。
     */
    public void startThreads();

    /**
     * 阻塞调用线程直到所有任务线程结束（Thread.join）。
     * 用于 StreamEnv 主线程等待。
     */
    public void awaitTermination() throws InterruptedException;

    /**
     * 终止所有任务线程（interrupt all threads）。
     * 由 shutdownHook 或 StreamEnv 清理阶段调用。幂等。
     * 若状态已为 FAILED（由任务线程异常设置），不覆盖为 STOPPED。
     */
    public void stop();

    /**
     * 注入全局 shutdown 钩子（由 StreamEnv 在装配阶段调用）。
     * 线程异常时触发此钩子，协调 Checkpoint/Web/任务线程的统一停止。
     */
    public void setShutdownHook(Runnable shutdownHook);

    /** 返回 TaskInfo 供 web 子域查询（只读） */
    public TaskInfo getTaskInfo();
}
```

## 核心流程（伪代码）

```
JobExecutor(plan, config):
  taskInfo = new TaskInfo(config.jobName, "INITIALIZING", plan.getAllTasks().size(), config)

JobExecutor.startThreads():
  for task in plan.getAllTasks():
    thread = new Thread(task)
    thread.setName("task-" + task.getNodeId())
    thread.setUncaughtExceptionHandler((t, e) -> {
      log.error("Task thread {} failed: {}", t.getName(), e.getMessage())
      taskInfo.status = "FAILED"    // 先标记 FAILED，再触发全局停止
      shutdownHook.run()            // fail-fast：触发全局停止
    })
    threads.add(thread)
    thread.start()
  taskInfo.status = "RUNNING"
  taskInfo.startTimeMs = System.currentTimeMillis()

JobExecutor.stop():
  for thread in threads: thread.interrupt()
  if taskInfo.status != "FAILED":   // 若已因异常标记 FAILED，不覆盖为 STOPPED
    taskInfo.status = "STOPPED"
```

## 依赖约束

- 依赖同域组件：`TaskPlan`（注入，不自行创建任务）
- 依赖 L2 定义：`StreamConfig`、`TaskInfo`
- 不依赖 checkpoint 子域
- 不依赖 web 子域
- 不依赖 state 子域
- 不依赖 metrics 子域
