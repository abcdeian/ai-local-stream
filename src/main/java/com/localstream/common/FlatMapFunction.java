package com.localstream.common;

import java.util.List;

/**
 * 一对多转换函数，输入一条数据，输出 0 到 N 条数据。
 * map 和 filter 语义均可通过此接口表达。
 */
public interface FlatMapFunction<T, R> {
    /** 返回空列表表示该条输入产生 0 条输出 */
    List<R> flatMap(T value);
}
