# L3 — web 领域

## 职责
提供内置 HTTP 服务，将任务运行信息（来自 runtime）、数据指标（来自 metrics）、Checkpoint 状态（来自 checkpoint）和运行日志聚合后以 HTML 页面和 JSON REST 接口对外暴露。随任务启动，随 JVM 退出停止。web 领域只做数据聚合与展示，不影响数据处理链路。

## 展示信息说明

### 一、任务执行状态
从 runtime 子域获取 `TaskInfo`，从 metrics 子域获取 `MetricsSnapshot`，展示以下内容：
- 任务名称、运行状态（RUNNING / STOPPED / FAILED）、节点总数
- 每个 Source 节点的读取总数和 RPS（`read_count` / `read_rps`）
- 每个 Sink 节点的写出总数和 RPS（`write_count` / `write_rps`）

### 二、Checkpoint 状态
从 checkpoint 子域获取 `CheckpointInfo`，展示以下内容：
- 已成功完成的 Checkpoint 总次数
- 最近一次 Checkpoint 的状态（SUCCESS / FAILED / NONE）
- 最近一次 Checkpoint 的完成时间
- 最近一次 Checkpoint 的存储路径

### 三、任务启动信息
从 runtime 子域的 `TaskInfo.config`（`StreamConfig`）提取，展示以下内容：
- 任务启动时间
- checkpoint 是否启用、触发周期、存储目录
- 队列容量、Web 端口等关键配置

### 四、运行日志
从 `StreamConfig.logFilePath` 指定的日志文件中读取，展示以下内容：
- 读取日志文件末尾 `WebConfig.logTailLines` 行（默认 500 行）
- 日志以原始文本形式展示（`<pre>` 标签），支持按需刷新

## 核心流程

### 启动 Web 服务
1. `StreamEnv` 完成 Checkpoint 装配后、启动任务线程前，根据 `WebConfig.enabled` 决定是否启动 web 服务
2. 使用 JDK 内置 `com.sun.net.httpserver.HttpServer` 在 `WebConfig.port` 端口监听
3. 注册路由：`GET /` 返回 HTML 主页；`GET /api/status` 返回 JSON 聚合数据；`GET /api/logs` 返回日志文本
4. 若端口被占用，打印警告日志，web 服务降级为不可用，不影响主任务

### 处理状态请求（/api/status 和 /）
1. 从 runtime 子域获取 `TaskInfo`
2. 从 metrics 子域获取 `MetricsSnapshot`
3. 从 checkpoint 子域获取 `CheckpointInfo`
4. 三部分数据聚合后，`/api/status` 接口返回 JSON；`/` 接口返回内嵌刷新脚本的 HTML 页面
5. HTML 页面通过 JS 定时轮询 `/api/status` 实现自动刷新（刷新间隔由 `WebConfig.refreshIntervalSeconds` 控制）

### 处理日志请求（/api/logs）
1. 读取 `StreamConfig.logFilePath` 指定文件末尾 `WebConfig.logTailLines` 行内容
2. 以纯文本形式返回；若文件不存在则返回空字符串，不抛出异常

## 组件划分

| 组件 | 职责 |
|------|------|
| WebServer | 管理 HttpServer 生命周期（启动、停止）；注册路由到对应 Handler |
| StatusHandler | 处理 `/`、`/api/status`、`/api/logs` 请求；聚合 TaskInfo + MetricsSnapshot + CheckpointInfo；渲染 HTML 或序列化 JSON；读取日志文件末尾内容 |

## 关键约束
- 仅使用 JDK 内置 `com.sun.net.httpserver`，不引入任何 Web 框架
- web 领域只读取 runtime、metrics、checkpoint 数据，不写入、不修改任何运行状态
- web 服务失败不影响主任务（端口占用降级，请求异常仅记录日志）
- JSON 序列化使用手工拼接字符串，不依赖第三方 JSON 库
- 日志读取仅读末尾固定行数，不全量加载文件，避免大文件 OOM
