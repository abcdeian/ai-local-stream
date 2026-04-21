# L3 — state 领域

## 职责
提供状态存储的抽象（`StateBackend` 接口）及当前的本地磁盘实现（`LocalDiskStateBackend`）。state 领域只处理状态数据的序列化写入和反序列化读出，不感知业务含义。

## 核心流程

### 保存状态
1. 接收 checkpoint 子域传入的 `checkpointId` 和全量状态 `Map<nodeId, Map<String,String>>`
2. 在 `checkpointDir/{checkpointId}/` 目录下为每个节点生成一个 properties 文件（`{nodeId}.properties`）
3. 将 `Map<String,String>` 按 key=value 格式写入文件
4. 写入完成后在目录下创建 `_SUCCESS` 标记文件，表示本次 checkpoint 完整

### 加载状态
1. 接收 `checkpointId`，定位 `checkpointDir/{checkpointId}/` 目录
2. 检查 `_SUCCESS` 文件存在；若不存在则抛出异常（文件损坏或未完成）
3. 读取目录下所有 `{nodeId}.properties` 文件，解析为 `Map<nodeId, Map<String,String>>` 返回

### 查询最新版本
1. 扫描 `checkpointDir/` 下所有子目录，找出含有 `_SUCCESS` 的最大数字目录名作为 latestCheckpointId
2. 若无合法目录则返回 -1

## 组件划分

| 组件 | 职责 |
|------|------|
| LocalDiskStateBackend | 实现 `StateBackend`；负责本地磁盘的状态文件读写，管理目录结构和 `_SUCCESS` 标记 |

## 关键约束
- state 领域只依赖 JDK 标准 IO，不引入任何序列化框架
- 状态值已由节点实现方序列化为 String，state 领域透明存取，不做二次解析
- 目录命名格式：`{checkpointDir}/{checkpointId}/`，checkpointId 为长整型数字
