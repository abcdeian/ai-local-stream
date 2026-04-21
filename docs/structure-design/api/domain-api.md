# L3 — api 领域

## 职责
为用户提供流水线编程入口，封装 DAG 构建过程，屏蔽内部拓扑细节。用户只需链式调用即可描述完整的数据处理流程。

## 核心流程

1. 用户创建 `StreamEnv`，可传入 `StreamConfig` 指定 checkpoint 周期、web 端口等配置
2. 用户调用 `env.addSource(sourceFunction)` 注册数据来源，返回 `DataSet`
3. 用户对 `DataSet` 链式调用算子（flatMap / keyBy / union），每次调用产生新的 `DataSet` 节点并向 DAG 注册一条边
4. 用户调用 `dataSet.sink(sinkFunction)` 注册输出目标，注册 SINK 节点
5. 用户调用 `env.start()` 触发 DAG 校验与任务启动，内部委托 dag 子域和 runtime 子域完成后续工作

## 组件划分

| 组件 | 职责 |
|------|------|
| StreamEnv | 入口类；持有 StreamConfig 和 DAG 构建上下文；提供 addSource / start 方法 |
| DataSet | 代表一条数据流；持有对应 OperatorNode 引用；提供 flatMap / keyBy / union / sink 链式方法 |

## 关键约束
- `DataSet` 每次算子调用都在内部创建并注册新的 `OperatorNode`，不做任何执行
- DAG 构建与校验由 dag 子域负责，api 领域只负责节点注册
- `start()` 调用后 api 领域不再持有运行时状态，生命周期交由 runtime 子域管理
