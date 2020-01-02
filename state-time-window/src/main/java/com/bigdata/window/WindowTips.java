package com.bigdata.window;

/**
 * 通过深入理解和熟练使用Assigner/Trigger/Evictor/Function以及Window State，可以很方便的解决业务中的一些需求问题
 * https://mp.weixin.qq.com/s/Y5T-c1DclgphLV4DwBV16w
 * 1、第一个例子，定时定量输出很简单，减少对外部数据库的访问，这样方式cpu利用率低（com.bigdata.sinkMultithreading.Multithreading）
 * 2、去重，使用session window去重新想法，但这样只能对间隔较小的重复数据去重
 * 3、大窗口的自定义触发（com.bigdata.ContinuousEventTimeTriggerDemo）
 * 4、在聚合上aggregatefunction优于process function （没有evictor前提下）
 * 5、改写细粒度的滑动窗口，结合这个文章（https://mp.weixin.qq.com/s/Q4k0xgPCOUQ-A2DQ-XaJgw）
 */
public class WindowTips {

    public static void main(String[] args) {

    }
}
