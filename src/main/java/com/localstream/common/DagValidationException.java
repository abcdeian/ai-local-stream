package com.localstream.common;

/** DAG 校验失败时抛出，如含环路、节点缺失、无 Sink 等。 */
public class DagValidationException extends RuntimeException {
    public DagValidationException(String message) {
        super(message);
    }
}
