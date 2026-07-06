# local-stream
这是一个用lsdd方法，通过AI生成的一个单机实时流计算引擎程序。

## 项目提示词
提示词1：生成整个程序
```text
/lsdd 实现一个单机的流式计算引擎。
用户能够通过java代码，编排流水线，并启动任务，单机长期运行。项目名字叫做：LocalStream
要求：
1，用java 8语言开发，maven项目
2，系统抽象数据流为dataset，支持java api编程接口：map，filter，keyby，union。当用户写完dataset的处理流程后，之后调用start方法，就启动了。支持多读多谢。后面有用户编程伪代码
3，系统支持checkpoint，checkpoint周期触发的时候，系统会把状态数据写入到本地磁盘
4，系统支持从历史状态恢复任务，
5，系统支持指标监控，监控数据流dag读取了多少数据，写出了多少数据
6，仅支持dag数据流处理，不支持循环
用户编程伪代码：
StreamEnv env = new StreamEnv();
DataSet source = env.addSource(new UserDataSource);
DataSet result = source.map(x -> convertToY(x)).filter(x -> x.isBlack()).keyby(x -> x.id, x -> sum(x));
result.sink(new UserSink());
env.start();
﻿
一层层的实现，并一层层的让我review，通过后，才执行下一层
```
提示词2：增加web ui
```text

/lsdd 用例中增加： 
1，通过任务的webui，可以看到任务的dag图形，那些是输入，那些是输出，一目了然。并且能够通过节点的名字看出来这个节点的功能，是map还是agg，还是union等等。
 2，能看到所有节点的输入输出的总数列表，按照node的拓扑排序。 
3，每个节点都有一个最近10分钟的rps的曲线图。 
4，页面每5秒自动刷新一下。
增加完用例后，开始按照layer慢慢生成，每层都需要给我review。
```