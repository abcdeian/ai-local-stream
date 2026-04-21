# L4 — CP-016 DagOptimizer

## 组件概述

所属领域：dag-optimizer

职责：对已通过校验的 `JobGraph` 执行死节点消除（Dead Node Elimination）优化：
从所有 SINK 节点出发，沿 `upstreamIds` 反向遍历，标记所有"有效节点"（即其输出最终能到达至少一个 SINK 的节点）；
将所有未被标记的节点从 `JobGraph` 中移除，并记录日志。
不改变有效节点的拓扑顺序，也不修改任何节点的内部属性。

## 内部结构

```
DagOptimizer（无状态，纯静态工具类风格）
 └── optimize(JobGraph) → void（原地修改 graph，移除死节点）
```

## 功能小块

### DagOptimizer

```java
public class DagOptimizer {

    /**
     * 对 JobGraph 执行死节点消除优化。
     * 从所有 SINK 节点出发反向 BFS，标记所有有效节点；
     * 移除所有无法到达任意 SINK 的节点，并同步更新 graph.nodeIndex。
     * 若无死节点则不做任何修改。
     * 调用前提：graph 已经过 DagValidator.validate() 校验（保证无环且含 SINK）。
     */
    public void optimize(JobGraph graph);

    /**
     * 反向 BFS 实现（私有）：
     * 从 SINK 节点集合出发，沿 upstreamIds 反向遍历，收集所有可达节点 ID。
     */
    private Set<String> collectLiveNodeIds(JobGraph graph);
}
```

## 核心流程（伪代码）

```
DagOptimizer.optimize(graph):
  liveIds = collectLiveNodeIds(graph)

  deadNodes = [ node | node in graph.nodes AND node.nodeId NOT in liveIds ]
  if deadNodes is empty: return    // 无死节点，无需优化

  for dead in deadNodes:
    log.info("DagOptimizer: removing dead node [{}] type={}", dead.nodeId, dead.type)

  graph.nodes.removeAll(deadNodes)
  graph.nodeIndex.keySet().retainAll(liveIds)


collectLiveNodeIds(graph):
  liveIds = new HashSet<String>()
  queue = [ node | node in graph.nodes AND node.type == SINK ]  // 从所有 SINK 出发

  while queue not empty:
    cur = queue.poll()
    if cur.nodeId in liveIds: continue       // 已访问，跳过
    liveIds.add(cur.nodeId)

    for upstreamId in cur.upstreamIds:
      upstream = graph.nodeIndex.get(upstreamId)
      if upstream != null AND upstream.nodeId NOT in liveIds:
        queue.add(upstream)

  return liveIds
```

## 示例

```
原始 DAG（用户误写了一个无 Sink 连接的分支）：

  source-A ──→ flatmap-1 ──→ sink-1       ← 有效链路
  source-B ──→ flatmap-2                   ← 死节点（flatmap-2 没有连接到任何 Sink）

优化后：

  source-A ──→ flatmap-1 ──→ sink-1
  // source-B 和 flatmap-2 均被移除
```

## 依赖约束

- 依赖 L2 定义：`JobGraph`、`OperatorNode`、`OperatorType`
- 不依赖其他子域组件
- 无成员变量，线程安全（可复用同一实例）
- 在 `DagValidator.validate()` 之后、`TaskPlan` 构造之前调用
