package com.localstream.state;

import com.localstream.common.StateBackend;
import com.localstream.util.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * StateBackend 接口的本地磁盘实现。
 * 将每次 Checkpoint 的全量状态以 properties 文件形式写入 {checkpointDir}/{checkpointId}/ 目录，
 * 并以 _SUCCESS 标记文件保证写入原子性。
 */
public class LocalDiskStateBackend implements StateBackend {

    private static final Logger log = Logger.getLogger(LocalDiskStateBackend.class);
    private static final String SUCCESS_FILE = "_SUCCESS";

    private final String checkpointDir;

    public LocalDiskStateBackend(String checkpointDir) {
        this.checkpointDir = checkpointDir;
    }

    @Override
    public void save(long checkpointId, Map<String, Map<String, String>> states) throws Exception {
        File dir = new File(checkpointDir, String.valueOf(checkpointId));
        dir.mkdirs();

        for (Map.Entry<String, Map<String, String>> entry : states.entrySet()) {
            String nodeId = entry.getKey();
            Map<String, String> stateMap = entry.getValue();
            File file = new File(dir, nodeId + ".properties");
            try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
                for (Map.Entry<String, String> kv : stateMap.entrySet()) {
                    // 转义换行和等号（简单处理）
                    writer.println(escapeKey(kv.getKey()) + "=" + escapeValue(kv.getValue()));
                }
            }
        }

        // 所有文件写完后，最后写入 _SUCCESS 标记
        new File(dir, SUCCESS_FILE).createNewFile();
        log.info("StateBackend: checkpoint {} saved to {}", checkpointId, dir.getAbsolutePath());
    }

    @Override
    public Map<String, Map<String, String>> load(long checkpointId) throws Exception {
        File dir = new File(checkpointDir, String.valueOf(checkpointId));
        File successFile = new File(dir, SUCCESS_FILE);
        if (!successFile.exists()) {
            throw new Exception("Checkpoint " + checkpointId
                    + " is incomplete or corrupted (no _SUCCESS file): " + dir.getAbsolutePath());
        }

        Map<String, Map<String, String>> result = new HashMap<>();
        File[] files = dir.listFiles((d, name) -> name.endsWith(".properties"));
        if (files != null) {
            for (File file : files) {
                String nodeId = file.getName().replace(".properties", "");
                Properties props = new Properties();
                try (FileReader reader = new FileReader(file)) {
                    props.load(reader);
                }
                Map<String, String> stateMap = new HashMap<>();
                for (String key : props.stringPropertyNames()) {
                    stateMap.put(key, props.getProperty(key));
                }
                result.put(nodeId, stateMap);
            }
        }
        log.info("StateBackend: checkpoint {} loaded, {} nodes", checkpointId, result.size());
        return result;
    }

    @Override
    public long latestCheckpointId() throws Exception {
        File dir = new File(checkpointDir);
        if (!dir.exists() || !dir.isDirectory()) return -1;

        long maxId = -1;
        File[] subDirs = dir.listFiles(File::isDirectory);
        if (subDirs != null) {
            for (File subDir : subDirs) {
                try {
                    long id = Long.parseLong(subDir.getName());
                    if (new File(subDir, SUCCESS_FILE).exists()) {
                        maxId = Math.max(maxId, id);
                    }
                } catch (NumberFormatException ignored) {
                    // 非数字目录名，跳过
                }
            }
        }
        return maxId;
    }

    @Override
    public String pathOf(long checkpointId) {
        return Paths.get(checkpointDir, String.valueOf(checkpointId)).toString();
    }

    private static String escapeKey(String key) {
        return key.replace("\\", "\\\\").replace("=", "\\=").replace("\n", "\\n");
    }

    private static String escapeValue(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\n", "\\n");
    }
}
