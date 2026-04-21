# L4 — CP-002 DataSet

## 组件概述

所属领域：api

职责：代表 DAG 中某个节点输出的数据流句柄。用户通过链式调用在其上注册下游算子，每次调用都在 DagBuilder 中新增一个 `OperatorNode` 并返回代表新节点输出的 `DataSet`。`DataSet` 本身不执行任何计算，只构建 DAG 结构。

## 内部结构

```
DataSet<T>
 ├── OperatorNode currentNode    // 当前节点（本 DataSet 对应的输出节点）
 └── StreamEnv env               // 反向持有 env，用于向 DagBuilder 注册新节点
```

## 功能小块

### DataSet\<T\>

```java
public class DataSet<T> {

    private final OperatorNode currentNode;
    private final StreamEnv env;

    /** 包私有构造，由 StreamEnv.addSource() 和各算子方法创建 */
    DataSet(OperatorNode currentNode, StreamEnv env);

    /**
     * 注册一个 FlatMap 算子节点。
     * 创建 OperatorNode(type=FLATMAP, function=function, upstream=currentNode.nodeId)，
     * 注册到 DagBuilder，返回新节点的 DataSet<R>。
     */
    public <R> DataSet<R> flatMap(FlatMapFunction<T, R> function);

    /**
     * 注册一个 KeyBy 聚合算子节点。
     * 创建 OperatorNode(type=KEYBY, function=aggregateFunction + keySelector, upstream=currentNode.nodeId)。
     * 注意：keySelector 和 aggregateFunction 打包为内部 KeyByConfig 存入 function 字段。
     * 返回新节点的 DataSet<R>。
     */
    public <K, R> DataSet<R> keyBy(KeySelector<T, K> keySelector, AggregateFunction<K, T, R> aggregateFunction);

    /**
     * 注册一个 Union 合流节点，将当前流与 other 流合并为一条流。
     * 创建 OperatorNode(type=UNION, upstream=[currentNode.nodeId, other.currentNode.nodeId])。
     * 返回合并后新节点的 DataSet<T>。
     */
    public DataSet<T> union(DataSet<T> other);

    /**
     * 注册一个 Sink 节点，终结当前流。无返回值。
     * 创建 OperatorNode(type=SINK, function=sinkFunction, upstream=currentNode.nodeId)。
     */
    public void sink(SinkFunction<T> sinkFunction);

    /** 获取当前节点（供内部使用） */
    OperatorNode getCurrentNode();
}
```

### KeyByConfig（引用 L2 定义）

`KeyByConfig` 已在 L2 `common-definitions.md` 中统一定义，此处不再重复。
`DataSet.keyBy()` 内部创建 `KeyByConfig` 实例，存入 `OperatorNode.function` 字段。

## 核心流程（伪代码）

```
DataSet.flatMap(function):
  node = new OperatorNode(FLATMAP, function, upstream=[currentNode.nodeId])
  env.getDagBuilder().addNode(node)
  return new DataSet(node, env)

DataSet.keyBy(keySelector, aggregateFunction):
  config = new KeyByConfig(keySelector, aggregateFunction)
  node = new OperatorNode(KEYBY, config, upstream=[currentNode.nodeId])
  env.getDagBuilder().addNode(node)
  return new DataSet(node, env)

DataSet.union(other):
  node = new OperatorNode(UNION, null, upstream=[currentNode.nodeId, other.currentNode.nodeId])
  env.getDagBuilder().addNode(node)
  return new DataSet(node, env)

DataSet.sink(sinkFunction):
  node = new OperatorNode(SINK, sinkFunction, upstream=[currentNode.nodeId])
  env.getDagBuilder().addNode(node)
  // 无返回，终结链式调用
```

## 依赖约束

- 依赖 dag 子域：`DagBuilder`（通过 `StreamEnv.getDagBuilder()` 间接访问）
- 依赖 L2 定义：`OperatorNode`、`OperatorType`、`FlatMapFunction`、`KeySelector`、`AggregateFunction`、`SinkFunction`、`KeyByConfig`
- 不依赖 runtime / checkpoint / metrics / web
