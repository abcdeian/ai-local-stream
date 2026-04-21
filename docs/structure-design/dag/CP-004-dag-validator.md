# L4 — CP-004 DagValidator

## 组件概述

所属领域：dag

职责：对 `DagBuilder` 产出的 `JobGraph` 执行合法性校验：
① 检查图是否为空；
② 执行拓扑排序（Kahn 算法）检测有向环路；
③ 校验图中至少存在一个 SINK 节点（无 SINK 则数据流无法输出）。
校验通过后将 `JobGraph.nodes` 重排为拓扑顺序（SOURCE 在前，SINK 在后），保证 runtime 可按序初始化；任意校验失败均抛出 `DagValidationException`。

## 内部结构

```
DagValidator（无状态，纯静态工具类风格）
 └── validate(JobGraph) → void（原地修改 graph.nodes 为拓扑序，或抛异常）
```

## 功能小块

### DagValidator

```java
public class DagValidator {

    /**
     * 对 JobGraph 执行合法性校验。校验顺序：
     * 1. 图是否为空 → DagValidationException("DAG is empty")
     * 2. Kahn 拓扑排序无环检测 → DagValidationException("DAG contains cycle")
     * 3. 是否含至少一个 SINK 节点 → DagValidationException("DAG has no sink node")
     * 校验通过：将 graph.nodes 替换为拓扑排序后的有序列表（SOURCE 在前，SINK 在后）。
     */
    public void validate(JobGraph graph);

    /**
     * Kahn 算法核心实现（私有）：
     * 1. 计算每个节点的入度（upstreamIds.size()）
     * 2. 将所有入度为 0 的节点加入队列
     * 3. 循环：取队首节点加入结果列表，遍历其下游节点入度 -1，入度变 0 则入队
     * 4. 若结果列表长度 < 总节点数，说明存在环路
     */
    private List<OperatorNode> kahnSort(JobGraph graph);
}
```

## 核心流程（伪代码）

```
DagValidator.validate(graph):
  if graph.nodes is empty → throw DagValidationException("DAG is empty")
  sorted = kahnSort(graph)
  if sorted.size != graph.nodes.size → throw DagValidationException("DAG contains cycle")
  hasSink = sorted.stream().anyMatch(n -> n.type == SINK)
  if not hasSink → throw DagValidationException("DAG has no sink node")
  graph.nodes = sorted     // 原地替换为拓扑序

kahnSort(graph):
  // 构建出度邻接表（每个节点的下游列表）
  outEdges: Map<nodeId, List<nodeId>> = {}
  inDegree: Map<nodeId, int> = {}
  for each node in graph.nodes:
    inDegree[node.nodeId] = node.upstreamIds.size()
    for upId in node.upstreamIds:
      outEdges[upId].add(node.nodeId)

  queue = [ node | inDegree[node.nodeId] == 0 ]   // 入度为 0 的节点入队
  result = []
  while queue not empty:
    cur = queue.poll()
    result.add(cur)
    for downId in outEdges[cur.nodeId]:
      inDegree[downId] -= 1
      if inDegree[downId] == 0:
        queue.add(graph.nodeIndex[downId])
  return result
```

## 依赖约束

- 依赖 L2 定义：`JobGraph`、`OperatorNode`、`DagValidationException`
- 不依赖其他子域组件
- 无成员变量，线程安全（可复用同一实例）
