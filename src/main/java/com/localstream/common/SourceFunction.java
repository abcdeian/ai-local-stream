package com.localstream.common;

/**
 * 用户实现此接口以提供数据来源。
 * 系统持续循环调用 fetch() 读取数据，返回 null 表示当前无数据。
 */
public interface SourceFunction<T> {
    /**
     * 系统持续循环调用此方法以读取数据。
     * 返回 null 表示当前无数据，系统短暂等待后再次调用。
     * 抛出 RuntimeException 时系统记录日志后继续调用。
     */
    T fetch();
}
