package com.bigdata.distinct;

import org.apache.flink.api.common.state.ValueState;

/**
 * 在流式去重中，需要保存大量的历史数据，这种方法是尽量减少内存的使用
 * HyperLogLog 是种去重算法，非精确去重算法
 * bitmap是精确去重算法
 * <p>
 * 在需要对数据进行去重计数的场景里，实现方式是将数据明细存储在集合的数据结构中。
 * 然而，随着数据随时间的不断累积，明细数据占用了大量的存储空间。使用 HyperLoglog 去重计数，
 * 在牺牲非常小准确性的情况下，可以极大的减少数据存储。
 */
public class HLLMain {
    private ValueState<Byte[]> hllState;

    public static void main(String[] args) {

//        ValueStateDescriptor<Byte[]> hllStateDescriptor = new ValueStateDescriptor<>(
//                "hll",
//                Types.OBJECT_ARRAY(Types.BYTE)
//        );
//
////        this.hllState = getRuntimeContext().getState(hllStateDescriptor);
//
//        //*******************以上再open初始化，下面再process中获取******************************************
//
//        HLL hll = null;
//        if (this.hllState.value() == null) {
//            hll = new HLL(14, 5);
//        } else {
//            hll = HLL.fromBytes(ArrayUtils.toPrimitive(this.hllState.value()));
//        }
//
//        this.hllState.update(ArrayUtils.toObject(hll.toBytes()));
//
    }
}
