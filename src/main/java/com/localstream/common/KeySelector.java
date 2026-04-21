package com.localstream.common;

/** 从数据中提取分组键。 */
public interface KeySelector<T, K> {
    K getKey(T value);
}
