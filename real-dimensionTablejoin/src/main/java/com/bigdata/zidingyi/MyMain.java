package com.bigdata.zidingyi;

/**
 * 异步IO的时候：有异步客户端用,没有的话自己写个线程模拟异步客户端
 * 通过线程池方式来帮助我们完成异步请求关键在于线程池的core大小如何设置，
 * 如果设置过大，会到导致创建很多个线程，势必会造成CPU的压力比较大，由于大多数情况下集群是没有做CPU隔离策略的，就会影响到其他任务；
 * 如果设置过小，在处理的速度上根不上就会导致任务阻塞。
 * 可以做一个粗略的估算：
 * 假如任务中单个Task需要做维表关联查询的数据每秒会产生1000条，也就是1000的TPS，
 * 我们希望能够在1s以内处理完这1000条数据，如果外部单次查询耗时是10ms,
 * 那我们就需要10个并发同时执行，也就是我们需要的coreSize 是10。
 */
public class MyMain {

    public static void main(String[] args) {


    }
}
