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
 * 从本地磁盘 Checkpoint 恢复 LocalStreamExample 的测试任务。
 *
 * 前置条件：
 *   先运行 LocalStreamExample，确保 /tmp/localstream-checkpoint 目录下
 *   已存在至少一个完整 Checkpoint（带 _SUCCESS 标记文件）。
 *
 * 验证逻辑：
 *   RestoredWordCountSink 捕获每个词的首次输出值；若状态恢复成功，
 *   首次见到的计数应 > 1（续算），而非从 1 开始（冷启动）。
 *   程序运行 10 秒后自动退出，并打印恢复验证报告。
 *
 * 运行方式：
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.localstream.example.LocalStreamRestoreExample
 * </pre>
 *
 * DAG 拓扑（与 LocalStreamExample 完全一致）：
 * <pre>
 *   Source1(200ms) ──┬──── Union ── FlatMap(分词) ── KeyBy(词频) ── Sink1(验证词频)
 *                    └──── Sink2(打印 Source1 原始句子)
 *   Source2(300ms) ──┘
 * </pre>
 */
public class LocalStreamRestoreExample {

    /** Checkpoint 目录：必须与 LocalStreamExample 使用同一路径 */
    private static final String CHECKPOINT_DIR = "/tmp/localstream-checkpoint";

    /** 恢复后运行时长（毫秒），10 秒后自动退出并打印验证报告 */
    private static final long RUN_DURATION_MS = 10_000;

    // -------------------------------------------------------
    // Source1: 与 LocalStreamExample 保持相同语料，复现原始数据流
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
    // Source2: 与 LocalStreamExample 保持相同语料
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
    // FlatMapFunction: 按空格拆分为单词列表
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
    // AggregateFunction: 有状态词频统计（与 LocalStreamExample 保持一致）
    // restoreState() 将 Checkpoint 中的计数写回 counts Map
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
            System.out.println("  [恢复] WordCountAgg 状态已从 Checkpoint 恢复，词条数=" + counts.size());
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
    // RestoredWordCountSink: 捕获每个词的首次计数值，用于验证恢复效果
    //   - 若 firstSeenCount > 1 → 恢复成功（续算）
    //   - 若 firstSeenCount == 1 → 冷启动（恢复失败或 Checkpoint 不存在）
    // -------------------------------------------------------
    static class RestoredWordCountSink implements SinkFunction<String> {
        /** key=词, value=首次见到的计数（验证用） */
        private final ConcurrentHashMap<String, Integer> firstSeenCounts = new ConcurrentHashMap<>();

        @Override
        public void invoke(String value) {
            // 格式: "word => count"
            System.out.println("  [词频] " + value);
            String[] parts = value.split(" => ");
            if (parts.length == 2) {
                String word  = parts[0].trim();
                int    count = Integer.parseInt(parts[1].trim());
                firstSeenCounts.putIfAbsent(word, count);
            }
        }

        /**
         * 打印验证报告：
         *   RESTORED  — 首次计数 > 1，说明从快照续算
         *   COLD_START — 首次计数 == 1，说明从零开始
         */
        void printReport() {
            System.out.println("\n========== [恢复验证报告] ==========");
            if (firstSeenCounts.isEmpty()) {
                System.out.println("  未收到任何输出，无法验证（Checkpoint 恢复可能失败或运行时间太短）");
            } else {
                int restored  = 0;
                int coldStart = 0;
                for (Map.Entry<String, Integer> e : firstSeenCounts.entrySet()) {
                    String status = e.getValue() > 1 ? "RESTORED  " : "COLD_START";
                    System.out.printf("  [%s] %-20s 首次计数=%d%n",
                            status, e.getKey(), e.getValue());
                    if (e.getValue() > 1) restored++;
                    else coldStart++;
                }
                System.out.println("  ────────────────────────────────");
                System.out.printf("  合计: %d 个词续算(RESTORED), %d 个词冷启动(COLD_START)%n",
                        restored, coldStart);
                System.out.println(coldStart == 0
                        ? "  结论: ✓ 状态恢复成功，所有词从 Checkpoint 续算"
                        : "  结论: ✗ 存在冷启动词，请确认 Checkpoint 目录和词条覆盖情况");
            }
            System.out.println("=====================================\n");
        }
    }

    // -------------------------------------------------------
    // Sink2: 打印 Source1 原始句子（演示 Source 扇出，与原示例一致）
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

        // ---- 1. 构建 StreamConfig（与 LocalStreamExample 完全一致）----
        StreamConfig config = new StreamConfig();
        config.jobName             = "WordCountDemo";          // 任务名称必须一致（Checkpoint 目录隔离依赖 jobName）
        config.checkpointEnabled   = true;
        config.checkpointDir       = CHECKPOINT_DIR;
        config.checkpointIntervalMs    = 60 * 1000;
        config.checkpointAckTimeoutMs  = 10_000;
        config.logFilePath         = "/tmp/localstream-restore.log";
        config.queueCapacity       = 256;

        WebConfig webConfig = new WebConfig();
        webConfig.enabled               = true;
        webConfig.port                  = 8081;               // 与原示例错开端口，方便对比
        webConfig.refreshIntervalSeconds = 3;
        webConfig.logTailLines          = 50;
        config.webConfig = webConfig;

        // ---- 2. 打印启动提示 ----
        System.out.println("=================================================");
        System.out.println("  LocalStream WordCount 恢复测试");
        System.out.println("  Checkpoint 恢复目录: " + CHECKPOINT_DIR);
        System.out.println("  Web 监控面板: http://localhost:" + webConfig.port);
        System.out.println("  日志文件: " + config.logFilePath);
        System.out.printf ("  程序将在 %.0f 秒后自动退出并打印验证报告%n", RUN_DURATION_MS / 1000.0);
        System.out.println("  ⚠ 请确保先运行过 LocalStreamExample 并产生过至少一次 Checkpoint！");
        System.out.println("=================================================\n");

        // ---- 3. 构建验证 Sink（需在 DAG 构建前创建，方便后续 printReport） ----
        RestoredWordCountSink restoredSink = new RestoredWordCountSink();

        // ---- 4. 定时 10 秒后打印报告并退出 ----
//        Timer shutdownTimer = new Timer("restore-demo-shutdown", true);
//        shutdownTimer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                restoredSink.printReport();
//                System.out.println("[恢复测试] 运行结束，退出。");
//                System.exit(0);
//            }
//        }, RUN_DURATION_MS);

        // ---- 5. 构建与 LocalStreamExample 相同的 DAG 拓扑 ----
        StreamEnv env = new StreamEnv(config);

        DataSet<String> source1 = env.addSource(new SentenceSource1());
        DataSet<String> source2 = env.addSource(new SentenceSource2());

        DataSet<String> merged  = source1.union(source2);
        DataSet<String> words   = merged.flatMap(new SplitWords());
        DataSet<String> counted = words.keyBy(new WordKeySelector(), new WordCountAgg());

        counted.sink(restoredSink);        // 使用验证 Sink
        source1.sink(new RawSentenceSink());

        // ---- 6. 启动（StreamEnv 内部自动检测 Checkpoint 并恢复状态） ----
        env.start();
    }
}
