# L3 — web 领域

## 职责
提供内置 HTTP 服务，将任务运行信息（来自 runtime）、数据指标（来自 metrics）、DAG 拓扑（来自 runtime/JobGraph）、Checkpoint 状态（来自 checkpoint）和运行日志聚合后以 HTML 页面和 JSON REST 接口对外暴露。随任务启动，随 JVM 退出停止。web 领域只做数据聚合与展示，不影响数据处理链路。

## 展示信息说明

### 一、DAG 拓扑图
从 runtime 子域获取 `JobGraph`，派生 `DagSnapshot` 后展示：
- 每个节点以图形方块呈现，节点名称（如 "source-1"、"map-2"、"keyby-1"）直接体现其功能类型
- SOURCE 节点与 SINK 节点在视觉上有明显区分（不同颜色或边框），连线体现数据流方向（从上游指向下游）
- 拓扑图由 JS 基于 `/api/dag` 接口数据在前端用 SVG 渲染

### 二、全节点输入/输出数据总数
从 metrics 子域获取 `MetricsSnapshot`，展示以下内容：
- 所有节点按拓扑顺序（`topologyOrder` 升序）排列的表格
- 每行包含：节点名称、节点类型、输入总条数（`inputCount`）、输出总条数（`outputCount`）
- SOURCE 节点的输入列显示"—"，SINK 节点的输出列显示"—"

### 三、每节点最近 10 分钟 RPS 曲线
从 metrics 子域获取 `MetricsSnapshot`，每个节点的 `rpsHistory`（最多 120 个 `RpsDataPoint`）：
- 以迷你折线图（sparkline）形式展示，宽度约 200px，嵌入表格行内
- 横轴为时间（最近 10 分钟），纵轴为每秒处理条数
- 由 JS 在前端用 `<canvas>` 或内联 SVG 绘制

### 四、Checkpoint 状态
从 checkpoint 子域获取 `CheckpointInfo`，展示以下内容：
- 已成功完成的 Checkpoint 总次数
- 最近一次 Checkpoint 的状态（SUCCESS / FAILED / NONE）
- 最近一次 Checkpoint 的完成时间
- 最近一次 Checkpoint 的存储路径

### 五、任务启动信息
从 runtime 子域的 `TaskInfo.config`（`StreamConfig`）提取，展示以下内容：
- 任务启动时间
- checkpoint 是否启用、触发周期、存储目录
- 队列容量、Web 端口等关键配置

### 六、运行日志
从 `StreamConfig.logFilePath` 指定的日志文件中读取，展示以下内容：
- 读取日志文件末尾 `WebConfig.logTailLines` 行（默认 500 行）
- 日志以原始文本形式展示（`<pre>` 标签），支持按需刷新

## 核心流程

### 启动 Web 服务
1. `StreamEnv` 完成 Checkpoint 装配后、启动任务线程前，根据 `WebConfig.enabled` 决定是否启动 web 服务
2. 使用 JDK 内置 `com.sun.net.httpserver.HttpServer` 在 `WebConfig.port` 端口监听
3. 注册路由：
   - `GET /` 返回 HTML 主页
   - `GET /api/status` 返回 JSON 聚合数据（TaskInfo + MetricsSnapshot + CheckpointInfo）
   - `GET /api/dag` 返回 JSON 格式的 `DagSnapshot`（节点列表及连接关系）
   - `GET /api/logs` 返回日志文本
4. 若端口被占用，打印警告日志，web 服务降级为不可用，不影响主任务

### 处理状态请求（/api/status）
1. 从 runtime 子域获取 `TaskInfo`
2. 从 metrics 子域获取 `MetricsSnapshot`（含所有节点的 `NodeMetricsSummary` 和 `rpsHistory`）
3. 从 checkpoint 子域获取 `CheckpointInfo`
4. 三部分数据聚合后序列化为 JSON 返回

### 处理 DAG 请求（/api/dag）
1. 从 runtime 子域获取 `JobGraph`
2. 遍历 `JobGraph.nodes`（已拓扑排序），逐一构造 `DagNodeView`（nodeId、name、type、upstreamIds）
3. 封装为 `DagSnapshot` 序列化为 JSON 返回

### 处理 HTML 请求（/）
1. 返回内置 HTML 页面，页面结构：
   - DAG 拓扑图区域（JS fetch `/api/dag` 后用 SVG 渲染）
   - 全节点指标表格（JS fetch `/api/status` 后渲染，含 RPS 迷你折线图）
   - Checkpoint 状态区域
   - 日志区域（fetch `/api/logs`）
2. 页面内嵌 JS 定时器，每 `WebConfig.refreshIntervalSeconds`（默认 5）秒轮询 `/api/status` 和 `/api/dag` 刷新全部数据

### 处理日志请求（/api/logs）
1. 读取 `StreamConfig.logFilePath` 指定文件末尾 `WebConfig.logTailLines` 行内容
2. 以纯文本形式返回；若文件不存在则返回空字符串，不抛出异常

## 组件划分

| 组件 | 职责 |
|------|------|
| WebServer | 管理 HttpServer 生命周期（启动、停止）；注册路由到对应 Handler |
| StatusHandler | 处理 `/`、`/api/status`、`/api/dag`、`/api/logs` 请求；聚合 TaskInfo + MetricsSnapshot + CheckpointInfo；从 JobGraph 构造 DagSnapshot；渲染 HTML（含 DAG SVG + RPS 折线 JS）或序列化 JSON；读取日志文件末尾内容 |

## 关键约束
- 仅使用 JDK 内置 `com.sun.net.httpserver`，不引入任何 Web 框架
- web 领域只读取 runtime、metrics、checkpoint 数据，不写入、不修改任何运行状态
- web 服务失败不影响主任务（端口占用降级，请求异常仅记录日志）
- JSON 序列化使用手工拼接字符串，不依赖第三方 JSON 库
- DAG 渲染和 RPS 折线图均在前端 JS 完成，服务端只输出数据；JS 代码内嵌于 HTML，不加载外部资源
- 日志读取仅读末尾固定行数，不全量加载文件，避免大文件 OOM
