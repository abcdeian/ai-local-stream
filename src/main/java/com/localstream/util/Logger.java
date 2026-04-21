package com.localstream.util;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 简单日志工具，同时输出到 System.out 和日志文件（若已初始化）。
 */
public class Logger {

    private static volatile PrintWriter fileWriter = null;
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /** 由 StreamEnv.start() 调用一次，初始化日志文件写入器 */
    public static synchronized void init(String logFilePath) {
        try {
            File file = new File(logFilePath);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            fileWriter = new PrintWriter(new FileWriter(file, true), true);
        } catch (Exception e) {
            System.err.println("[Logger] Failed to init log file: " + logFilePath + " - " + e.getMessage());
        }
    }

    private final String name;

    private Logger(String name) {
        this.name = name;
    }

    public static Logger getLogger(Class<?> clazz) {
        return new Logger(clazz.getSimpleName());
    }

    public static Logger getLogger(String name) {
        return new Logger(name);
    }

    public void info(String msg, Object... args) {
        log("INFO ", msg, args);
    }

    public void warn(String msg, Object... args) {
        log("WARN ", msg, args);
    }

    public void error(String msg, Object... args) {
        log("ERROR", msg, args);
    }

    private void log(String level, String msg, Object... args) {
        String formatted = format(msg, args);
        String line = String.format("[%s] [%s] [%s] %s",
                SDF.format(new Date()), level, name, formatted);
        System.out.println(line);
        PrintWriter w = fileWriter;
        if (w != null) {
            synchronized (Logger.class) {
                w.println(line);
                w.flush();
            }
        }
    }

    /** 简单 {} 占位符替换 */
    private static String format(String msg, Object... args) {
        if (args == null || args.length == 0) return msg;
        StringBuilder sb = new StringBuilder();
        int argIdx = 0;
        int i = 0;
        while (i < msg.length()) {
            if (i < msg.length() - 1 && msg.charAt(i) == '{' && msg.charAt(i + 1) == '}') {
                sb.append(argIdx < args.length ? args[argIdx++] : "{}");
                i += 2;
            } else {
                sb.append(msg.charAt(i++));
            }
        }
        return sb.toString();
    }
}
