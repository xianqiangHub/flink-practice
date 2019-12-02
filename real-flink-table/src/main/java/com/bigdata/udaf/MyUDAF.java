package com.bigdata.udaf;


import org.apache.flink.table.functions.AggregateFunction;

/**
 * 自定义聚合函数是一个增量聚合的过程，中间结果保存在状态中，能够保证其容错与一致性语义。
 * 用户自定义聚合函数继承AggregateFunction即可，
 * 至少实现createAccumulator 、accumulate 、getValue这三个方法，其他方法都是可选的。
 */
public class MyUDAF extends AggregateFunction<Integer, TimeAndStatus> {


    @Override
    public Integer getValue(TimeAndStatus accumulator) {
        return accumulator.getStatus();
    }

    @Override
    public TimeAndStatus createAccumulator() {
        return new TimeAndStatus();
    }

    // * public void accumulate(ACC accumulator, [user defined inputs])}
    public void accumulate(TimeAndStatus acc, Integer status, Long time) {

        //业务上，那最新的状态
        if (time > acc.getTimes()) {
            acc.setStatus(status);
            acc.setTimes(time);
        }
    }

//    如果其接受到的是撤回流那么就必须实现该方法
//    该方法表示将需要撤回的数据从中间结果中去除掉，Flink中默认实现了一些撤回的函数，例如SumWithRetractAggFunction
//    比喻说sql1上游是消费kafka 非撤回流，所以在定义LatestTimeUdf 并没有定义retract,sql2 消费sql1的输出
//    在生成Function 会判断上游消费的数据是否是可撤回来决定是否生成retract方法，那么在其内部会生成retract方法，这部分是代码自动生成的
//    publicvoid retract(ACC accumulator,[user defined inputs])}

}
