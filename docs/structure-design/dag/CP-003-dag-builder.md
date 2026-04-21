# L4 — CP-003 DagBuilder

## 组件概述

所属领域：dag

职责：接收 api 子域逐步注册的 `OperatorNode`，维护完整的节点集合与上下游边关系，最终产出供 `DagValidator` 校验的图结构和供 runtime 使用的 `JobGraph`。

## 内部结构

```
DagBuilder
 ├── List<OperatorNode> nodes          // 按注册顺序保存的所有节点
 └── Map<String, OperatorNode> index   // nodeId → OperatorNode 快速查找
```

## 功能小块

### DagBuilder

```java
public class DagBuilder {

    private final List<OperatorNode> nodes = new ArrayList<>();
    private final Map<String, OperatorNode> index = new LinkedHashMap<>();

    /**
     * 注册一个新节点。
     * 生成全局唯一 nodeId（UUID 短串），写入 node.nodeId；
     * 按类型+当前序号生成可读名称，写入 node.name（如 "source-1"、"keyby-3"）。
     * 加入 nodes 列表和 index 映射。
     * 线程不安全（单线程 DAG 构建场景，无需加锁）。
     */
    public void addNode(OperatorNode node);

    /**
     * 构建并返回 JobGraph。
     * 将当前 nodes 和 index 封装为 JobGraph 返回。
     * 此时尚未执行拓扑排序，图可能含环（校验由 DagValidator 完成）。
     * 调用后 DagBuilder 仍可继续注册节点（幂等性不保证，实践中只调用一次）。
     */
    public JobGraph build();

    /** 返回当前已注册的节点数量（用于 TaskInfo.totalNodes） */
    public int nodeCount();
}
```

## 核心流程（伪代码）

```
DagBuilder.addNode(node):
  node.nodeId = UUID.randomUUID().toString().substring(0, 8)
  node.name   = node.type.name().toLowerCase() + "-" + (nodes.size() + 1)
  // 例：第1个SOURCE节点 → "source-1"；第3个KEYBY节点 → "keyby-3"
  nodes.add(node)
  index.put(node.nodeId, node)

DagBuilder.build():
  graph = new JobGraph()
  graph.nodes = new ArrayList<>(nodes)      // 此时顺序为注册顺序，非拓扑序
  graph.nodeIndex = new HashMap<>(index)
  return graph
  // 注：DagValidator 会对 graph.nodes 重排为拓扑序
```

## 依赖约束

- 依赖 L2 定义：`OperatorNode`、`JobGraph`
- 不依赖其他子域组件
