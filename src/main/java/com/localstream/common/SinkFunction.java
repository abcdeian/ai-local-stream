package com.localstream.common;

/** 用户实现此接口以消费数据。 */
public interface SinkFunction<T> {
    void invoke(T value);
}
