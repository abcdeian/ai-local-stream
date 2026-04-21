# L3 — dag-optimizer 领域

## 职责
对已通过合法性校验的 `JobGraph` 执行图优化，当前实现为**死节点消除（Dead Node Elimination）**：移除所有无法到达任意 SINK 节点的死节点，减少 runtime 阶段不必要的线程和资源分配。dag-optimizer 领域不修改任何节点的内部属性，只调整图中节点的集合。

## 核心流程

1. `env.start()` 在 DAG 校验（`DagValidator`）完成后，将 `JobGraph` 传入 dag-optimizer 领域
2. `DagOptimizer` 从所有 SINK 节点出发，沿 `upstreamIds` 反向 BFS，标记所有"有效节点"（其输出最终能到达至少一个 SINK）
3. 将所有未被标记的节点从 `JobGraph.nodes` 和 `JobGraph.nodeIndex` 中移除，并记录日志
4. 若无死节点，不做任何修改，直接返回
5. 优化完成后将 `JobGraph` 交给 runtime 子域进行任务初始化

## 组件划分

| 组件 | 职责 |
|------|------|
| DagOptimizer | 对 JobGraph 执行死节点消除；从 SINK 出发反向 BFS 标记有效节点；移除死节点并更新 nodeIndex |

## 关键约束
- dag-optimizer 在 `DagValidator.validate()` 之后、`TaskPlan` 构造之前调用，依赖校验保证无环且含 SINK
- 只原地修改 `JobGraph.nodes` 和 `JobGraph.nodeIndex`，不创建新对象，不改变有效节点的拓扑顺序
- 无状态（纯静态工具类风格），线程安全，可复用同一实例
- 死节点消除失败时直接向上抛出异常（视为 `JobStartException`），拒绝启动
