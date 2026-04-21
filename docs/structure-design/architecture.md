# L2 — 架构设计 | LocalStream

## 系统总体架构

LocalStream 是一个单机流式计算引擎，分为 8 个子域，各子域职责单一，通过明确的接口边界协作。

```
┌─────────────────────────────────────────────────────────────┐
│                        用户代码层                            │
│   StreamEnv  +  DataSet  +  用户自定义 Source/Sink/Function │
└──────────────────────┬──────────────────────────────────────┘
                       │ 构建 DAG
                       ▼
┌──────────────────────────────────┐
│          api 子域                │
│  编程模型入口、算子链构建、整体装配 │
└──────────┬───────────────────────┘
           │ 提交 DAG 拓扑
           ▼
┌──────────────────────────────────┐
│          dag 子域                │
│  DAG 表示、合法性校验（无环检测） │
└──────────┬───────────────────────┘
           │ 校验后 JobGraph
           ▼
┌──────────────────────────────────┐
│       dag-optimizer 子域         │
│  DAG 优化（死节点消除）           │
└──────────┬───────────────────────┘
           │ 优化后 JobGraph
           ▼
┌──────────────────────────────────────────────────────────────┐
│                       runtime 子域                           │
│  节点实例化、线程调度、节点间数据传递（有界队列）             │
│                                                              │
│   Source Thread(s) → Queue → Operator Thread(s) → Queue → Sink Thread(s)  │
└─────┬──────────────────────────┬───────────────────────────┘
      │ 注册可检查点节点          │ 上报读写事件
      ▼                          ▼
┌─────────────────┐    ┌─────────────────┐
│  checkpoint 子域 │    │   metrics 子域   │
│  周期触发、状态  │    │  读取/写出计数   │
│  收集与协调      │    │  查询接口        │
└────────┬────────┘    └────────┬────────┘
         │ 调用存储抽象          │ 提供指标数据
         ▼                      │
┌─────────────────────────────┐  │  ┌──────────────────────────────┐
│         state 子域           │  └─▶│          web 子域             │
│  StateBackend 抽象           │     │  内置 HTTP Server             │
│  LocalDiskStateBackend 实现  │     │  聚合 metrics + runtime 信息  │
└─────────────────────────────┘     │  对浏览器提供页面与 REST 接口  │
                                    └──────────────────────────────┘
```

---

## 子域职责边界

### api 子域
- **职责**：提供用户编程模型入口，包括 `StreamEnv`、`DataSet` 及所有算子链式调用；同时作为整体系统装配点，协调各子域组件的创建与启动
- **输入**：用户代码调用
- **输出**：向 dag 子域提交由算子节点构成的逻辑拓扑；协调 dag-optimizer / runtime / checkpoint / metrics / web 子域完成任务启动

### dag 子域
- **职责**：表示、存储和校验有向无环图（DAG）结构
- **输入**：api 子域提交的逻辑节点和边关系
- **输出**：校验通过的 DAG 拓扑（`JobGraph`），供 dag-optimizer 子域处理

### dag-optimizer 子域
- **职责**：对已校验的 `JobGraph` 执行优化处理，当前实现为死节点消除（Dead Node Elimination）
- **输入**：dag 子域产出的校验后 `JobGraph`
- **输出**：优化后的 `JobGraph`，供 runtime 子域使用

### runtime 子域
- **职责**：将 DAG 节点实例化为可执行任务，分配线程，通过有界队列在节点间传递数据；负责整体任务生命周期管理
- **输入**：dag-optimizer 子域的优化后 `JobGraph`
- **输出**：
  - 以 nodeId 为 key 向 checkpoint 子域注册 KEYBY 节点的 AggregateFunction（实现 Checkpointable）
  - 向 metrics 子域上报 Source/Sink 的数据读写事件
  - 向 web 子域提供 `TaskInfo` 任务状态查询能力

### checkpoint 子域
- **职责**：通过 Barrier 驱动快照流程，协调所有非 Source 节点 ack 后汇总状态持久化；支持从历史版本恢复状态
- **输入**：runtime 提供的 checkpointableMap（`Map<nodeId, Checkpointable>`）；state 子域提供的存储能力
- **输出**：持久化的 Checkpoint 记录；恢复时向 runtime 提供各节点历史状态

### state 子域
- **职责**：提供状态存储抽象（`StateBackend`），当前实现为本地磁盘存储
- **输入**：checkpoint 子域调用 read/write 接口
- **输出**：状态数据的序列化存储与反序列化读取

### metrics 子域
- **职责**：维护各 Source 节点读取计数和各 Sink 节点写出计数，对外提供查询接口
- **输入**：runtime 子域的读写事件上报
- **输出**：`MetricsSnapshot` 查询结果

### web 子域
- **职责**：提供内置 HTTP 服务，将任务运行信息和数据指标聚合后以 Web 页面和 REST 接口形式对外暴露；随任务启动，随 JVM 退出停止
- **输入**：metrics 子域提供的 `MetricsSnapshot`；runtime 子域提供的 `TaskInfo` 任务状态信息
- **输出**：可在浏览器访问的 HTML 页面（含任务信息、Source/Sink 计数，定时自动刷新）；JSON REST 接口

---

## 子域间调用关系

```
api        ──提交拓扑──────▶  dag
dag        ──校验JobGraph──▶  dag-optimizer
dag-optimizer ──优化JobGraph──▶  runtime
runtime    ──注册节点──────▶  checkpoint
runtime    ──上报事件──────▶  metrics
checkpoint ──读写状态──────▶  state
checkpoint ──恢复状态──────▶  runtime（通过 restoreState() 注入）
web        ──查询指标──────▶  metrics
web        ──查询任务──────▶  runtime
```

**禁止的调用方向**：
- dag / dag-optimizer / checkpoint / state / metrics / web 不得反向调用 api
- state 不得调用 checkpoint
- metrics 不得调用 runtime
- web 不得调用 dag / dag-optimizer / checkpoint / state

---

## 数据流转路径（运行时）

```
Source Thread
  │  从 SourceFunction 读取数据
  │  上报读取计数 → metrics
  ▼
OutputQueue（有界阻塞队列）
  ▼
Operator Thread（flatmap / keyby / union）
  │  执行算子逻辑
  │  keyby 节点维护聚合状态（收到 CheckpointBarrier 后通过 CheckpointAckListener 上报状态快照）
  ▼
OutputQueue
  ▼
Sink Thread
  │  调用 SinkFunction 写出数据
  │  上报写出计数 → metrics
```

---

## 包结构规划

```
com.localstream
├── api              # api 子域
├── dag              # dag 子域
├── optimizer        # dag-optimizer 子域
├── runtime          # runtime 子域
├── checkpoint       # checkpoint 子域
├── state            # state 子域
├── metrics          # metrics 子域
└── web              # web 子域
```
