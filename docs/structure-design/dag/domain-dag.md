# L3 — dag 领域

## 职责
接收 api 子域提交的算子节点和边关系，构建 DAG 拓扑结构，执行合法性校验（无环检测、SINK 存在性检查），产出校验通过的 `JobGraph` 供 dag-optimizer 子域处理。

## 核心流程

1. api 子域每次注册算子节点时，dag 领域将 `OperatorNode` 加入节点集合，同时记录上下游边关系；在 addNode() 阶段自动为节点赋予 `nodeId`（UUID 短串）和 `name`（类型+序号）
2. `env.start()` 调用时，dag 领域执行拓扑排序（Kahn 算法）：若存在环路则抛出 `DagValidationException`，终止启动；若无 SINK 节点则同样抛出 `DagValidationException`
3. 校验通过后，将拓扑排序后的节点列表和索引封装为 `JobGraph`，交给 dag-optimizer 子域进行优化

## 组件划分

| 组件 | 职责 |
|------|------|
| DagBuilder | 接收节点注册请求，维护节点列表和邻接关系；自动生成 nodeId 和 name；构建原始有向图 |
| DagValidator | 对 DagBuilder 产出的图执行拓扑排序无环检测及 SINK 存在性校验；校验失败时抛出 `DagValidationException` |

## 关键约束
- dag 领域不感知算子的具体类型（SOURCE / FLATMAP 等），只关注节点和边
- 节点 ID 和 name 均由 DagBuilder.addNode() 在注册时自动生成，dag 领域内部统一管理
- `JobGraph` 中节点顺序严格按拓扑排序，dag-optimizer 和 runtime 子域均可直接按序处理
