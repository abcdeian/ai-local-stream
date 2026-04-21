package com.localstream.example;

import com.localstream.api.DataSet;
import com.localstream.api.StreamEnv;
import com.localstream.common.AggregateFunction;
import com.localstream.common.FlatMapFunction;
import com.localstream.common.KeySelector;
import com.localstream.common.SinkFunction;
import com.localstream.common.SourceFunction;
import com.localstream.common.StreamConfig;
import com.localstream.common.WebConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LocalStream 可运行完整演示示例。
 *
 * 演示功能：
 *   - 两路 Source（不同频率产生英文句子）
 *   - Union 合流（2→1）
 *   - FlatMap 分词
 *   - KeyBy 有状态词频统计（AggregateFunction + Checkpoint 状态快照与恢复）
 *   - Sink 打印词频结果
 *   - Source 扇出（Source1 同时向 Union 和原始句子 Sink 输出，演示一对多下游）
 *   - Checkpoint 自动触发（每 5 秒）
 *   - Web 监控面板
 *
 * DAG 拓扑：
 * <pre>
 *   Source1(200ms) ──┬──── Union ── FlatMap(分词) ── KeyBy(词频) ── Sink1(打印词频)
 *                    └──── Sink2(打印 Source1 原始句子)     ← 扇出演示
 *   Source2(300ms) ──┘
 * </pre>
 *
 * 运行方法：
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.localstream.example.LocalStreamExample
 * </pre>
 * 或打包后：
 * <pre>
 *   mvn package -DskipTests
 *   java -jar target/ai-local-stream-1.0-SNAPSHOT.jar
 * </pre>
 *
 * Web 监控面板（程序启动后访问）：
 *   http://localhost:8080
 *
 * 程序将自动运行 15 秒后退出（演示有界运行）。
 */
public class LocalStreamExample {

    // -------------------------------------------------------
    // Source1: 每 200ms 产生一条英文句子
    // -------------------------------------------------------
    static class SentenceSource1 implements SourceFunction<String> {
        private static final List<String> SENTENCES = Arrays.asList(
                "hello world hello java",
                "stream processing is fun",
                "hello stream hello world",
                "java is a great language",
                "local stream engine rocks"
        );
        private final AtomicInteger idx = new AtomicInteger(0);

        @Override
        public String fetch() {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            return SENTENCES.get(idx.getAndIncrement() % SENTENCES.size());
        }
    }

    // -------------------------------------------------------
    // Source2: 每 300ms 产生一条英文句子
    // -------------------------------------------------------
    static class SentenceSource2 implements SourceFunction<String> {
        private static final List<String> SENTENCES = Arrays.asList(
                "data pipeline is powerful",
                "checkpoint saves your state",
                "hello checkpoint hello save",
                "metrics show the throughput",
                "web dashboard is easy to use"
        );
        private final AtomicInteger idx = new AtomicInteger(0);

        @Override
        public String fetch() {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            return SENTENCES.get(idx.getAndIncrement() % SENTENCES.size());
        }
    }

    // -------------------------------------------------------
    // FlatMapFunction: 按空格将句子拆分为单词列表
    // -------------------------------------------------------
    static class SplitWords implements FlatMapFunction<String, String> {
        @Override
        public List<String> flatMap(String sentence) {
            if (sentence == null || sentence.trim().isEmpty()) {
                return Collections.emptyList();
            }
            return Arrays.asList(sentence.trim().split("\\s+"));
        }
    }

    // -------------------------------------------------------
    // AggregateFunction: 有状态词频统计（支持 Checkpoint 快照与恢复）
    // 每收到一个 word，对应计数 +1，返回 "word => count" 字符串
    // -------------------------------------------------------
    static class WordCountAgg implements AggregateFunction<String, String, String> {
        private final ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();

        @Override
        public List<String> add(String key, String value) {
            int count = counts.merge(key, 1, Integer::sum);
            List<String> result = new ArrayList<>();
            result.add(key + " => " + count);
            return result;
        }

        @Override
        public Map<String, String> snapshotState() {
            Map<String, String> snap = new HashMap<>();
            counts.forEach((k, v) -> snap.put(k, String.valueOf(v)));
            return snap;
        }

        @Override
        public void restoreState(Map<String, String> state) {
            counts.clear();
            state.forEach((k, v) -> counts.put(k, Integer.parseInt(v)));
        }
    }

    // -------------------------------------------------------
    // KeySelector: 以单词本身作为 Key
    // -------------------------------------------------------
    static class WordKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String word) {
            return word;
        }
    }

    // -------------------------------------------------------
    // Sink1: 打印词频统计结果
    // -------------------------------------------------------
    static class WordCountSink implements SinkFunction<String> {
        @Override
        public void invoke(String value) {
            System.out.println("  [词频] " + value);
        }
    }

    // -------------------------------------------------------
    // Sink2: 打印 Source1 原始句子（演示 Source 扇出）
    // -------------------------------------------------------
    static class RawSentenceSink implements SinkFunction<String> {
        @Override
        public void invoke(String sentence) {
            System.out.println("  [Source1 原句] " + sentence);
        }
    }

    // -------------------------------------------------------
    // main 入口
    // -------------------------------------------------------
    public static void main(String[] args) throws Exception {

        // ---- 1. 构建 StreamConfig ----
        StreamConfig config = new StreamConfig();
        config.jobName = "WordCountDemo";
        config.checkpointEnabled = true;
        config.checkpointDir = "/tmp/localstream-checkpoint";
        config.checkpointIntervalMs = 60*1000;       // 每 5 秒执行一次 Checkpoint
        config.checkpointAckTimeoutMs = 10000;    // 等待 ack 超时 10 秒
        config.logFilePath = "/tmp/localstream-demo.log";
        config.queueCapacity = 256;

        WebConfig webConfig = new WebConfig();
        webConfig.enabled = true;
        webConfig.port = 8080;
        webConfig.refreshIntervalSeconds = 3;
        webConfig.logTailLines = 50;
        config.webConfig = webConfig;

        // ---- 2. 打印启动提示 ----
        System.out.println("=================================================");
        System.out.println("  LocalStream WordCount Demo");
        System.out.println("  Web 监控面板: http://localhost:" + webConfig.port);
        System.out.println("  Checkpoint 目录: " + config.checkpointDir);
        System.out.println("  日志文件: " + config.logFilePath);
        System.out.println("  程序将在 15 秒后自动退出（演示有界运行）");
        System.out.println("=================================================\n");

        // ---- 3. 15 秒后自动退出（演示用，timer 为 daemon 线程） ----
//        Timer shutdownTimer = new Timer("demo-shutdown-timer", true);
//        shutdownTimer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println("\n[Demo] 15 秒演示结束，自动退出...");
//                System.exit(0);
//            }
//        }, 15_000);

        // ---- 4. 构建 DAG ----
        StreamEnv env = new StreamEnv(config);

        // 两路 Source
        DataSet<String> source1 = env.addSource(new SentenceSource1());
        DataSet<String> source2 = env.addSource(new SentenceSource2());

        // Union 合流（2 路 → 1 路）
        DataSet<String> merged = source1.union(source2);

        // FlatMap 分词
        DataSet<String> words = merged.flatMap(new SplitWords());

        // KeyBy 有状态词频统计
        DataSet<String> counted = words.keyBy(new WordKeySelector(), new WordCountAgg());

        // Sink1: 打印词频结果
        counted.sink(new WordCountSink());

        // Sink2: Source1 扇出 → 打印 Source1 原始句子
        source1.sink(new RawSentenceSink());

        // ---- 5. 启动（阻塞直到所有线程结束 或 System.exit） ----
        env.start();
    }
}
