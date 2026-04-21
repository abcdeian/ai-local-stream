# L4 — CP-012 LocalDiskStateBackend

## 组件概述

所属领域：state

职责：`StateBackend` 接口的本地磁盘实现。将每次 Checkpoint 的全量状态以 properties 文件形式写入 `{checkpointDir}/{checkpointId}/` 目录，并以 `_SUCCESS` 标记文件保证写入原子性。支持按版本加载状态，支持查询最新版本。

## 内部结构

```
LocalDiskStateBackend（实现 StateBackend）
 └── String checkpointDir    // 根路径，由 StreamConfig 传入
```

## 功能小块

### LocalDiskStateBackend

```java
public class LocalDiskStateBackend implements StateBackend {

    public LocalDiskStateBackend(String checkpointDir);

    /**
     * 将全量状态持久化到 {checkpointDir}/{checkpointId}/ 目录。
     * 每个节点生成 {nodeId}.properties 文件（key=value 格式）。
     * 所有文件写完后写入 _SUCCESS 标记文件。
     * 任何 IO 异常向上抛（CheckpointCoordinator 捕获处理）。
     */
    @Override
    public void save(long checkpointId, Map<String, Map<String, String>> states) throws Exception;

    /**
     * 加载 {checkpointDir}/{checkpointId}/ 目录下的所有状态。
     * 首先检查 _SUCCESS 文件是否存在；不存在则抛 Exception（文件损坏或未完成）。
     * 读取所有 {nodeId}.properties 文件，解析为 Map<nodeId, Map<String,String>>。
     */
    @Override
    public Map<String, Map<String, String>> load(long checkpointId) throws Exception;

    /**
     * 扫描 checkpointDir 下所有含 _SUCCESS 标记的子目录，
     * 返回目录名（纯数字）中最大值作为 latestCheckpointId。
     * 无合法目录时返回 -1。
     */
    @Override
    public long latestCheckpointId() throws Exception;

    /**
     * 返回指定 checkpointId 对应的存储路径字符串（供 CheckpointInfo 展示用）。
     * 格式：{checkpointDir}/{checkpointId}
     */
    @Override
    public String pathOf(long checkpointId);
}
```

## 核心流程（伪代码）

```
save(checkpointId, states):
  dir = Paths.get(checkpointDir, String.valueOf(checkpointId))
  Files.createDirectories(dir)

  for (nodeId, stateMap) in states:
    file = dir.resolve(nodeId + ".properties")
    writer = new FileWriter(file)
    for (k, v) in stateMap:
      writer.write(k + "=" + v + "\n")
    writer.close()

  // 所有文件写完后，最后写入 _SUCCESS
  Files.createFile(dir.resolve("_SUCCESS"))

load(checkpointId):
  dir = Paths.get(checkpointDir, String.valueOf(checkpointId))
  if not Files.exists(dir.resolve("_SUCCESS")):
    throw new Exception("Checkpoint " + checkpointId + " is incomplete or corrupted")

  result = new HashMap<>()
  for file in dir.listFiles("*.properties"):
    nodeId = file.name.replace(".properties", "")
    props = new Properties()
    props.load(new FileReader(file))
    result.put(nodeId, props.toMap())
  return result

latestCheckpointId():
  dir = new File(checkpointDir)
  if not dir.exists(): return -1
  maxId = -1
  for subDir in dir.listFiles(File::isDirectory):
    if subDir has _SUCCESS file and subDir.name is numeric:
      id = Long.parseLong(subDir.name)
      maxId = max(maxId, id)
  return maxId
```

## 依赖约束

- 实现 L2 定义的 `StateBackend` 接口
- 仅依赖 JDK 标准 IO（`java.io`、`java.nio.file`），不引入任何序列化框架
- 线程安全性：同一时刻只有一个 Checkpoint 执行（由 `CheckpointCoordinator.executing` 保证），无需内部加锁
