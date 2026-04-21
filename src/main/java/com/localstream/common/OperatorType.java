package com.localstream.common;

/** 算子节点的类型标识。 */
public enum OperatorType {
    SOURCE,   // 数据来源节点
    FLATMAP,  // 一对多转换节点（涵盖 map/filter 语义）
    KEYBY,    // 按键聚合节点
    UNION,    // 合流节点
    SINK      // 数据输出节点
}
