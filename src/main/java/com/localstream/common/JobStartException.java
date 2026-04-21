package com.localstream.common;

/** 任务启动时节点初始化失败时抛出。 */
public class JobStartException extends RuntimeException {
    public JobStartException(String message) {
        super(message);
    }

    public JobStartException(String message, Throwable cause) {
        super(message, cause);
    }
}
