package com.bigdata.sql;

/**
 * 了解sql执行之后，底层flink如何用算子实现
 * sql可以在client直接执行也可以流注册成table，tenv执行，初步发现client的SQL也是通过tenv的query执行
 * sql的join分为global join & window join
 * window join是通过流的interval join实现，即一条流和另一条流的一个时间范围内进行join
 * 例如：interval用流实现订单和地址的join，地址流比订单流迟到1s-5s,实现join，用SQL写为
 * select o.userid,a.addrid  from orderTable o left join addressTable a on o.id = a.id
 * and o.rtt between a.rt - interval '1' second and a.rt - interval '5' secnod
 * 可以看到interval只支持inner join ，time window 支持多种join，并且时间条件是 and
 *
 * <p>
 * global join是通过nonwindow join 实现无限流连接，数据都保存在状态中，默认是不会过期的
 * 核心类是 NonWindowInnerJoin 核心处理方法是processElement
 * 基本思路是两条流进行connect，使用Coprocessfunction，每条流都可以获取对方状态，获取对方的数据
 * 找到符合条件的输出
 * <p>
 * 一个问题就是时间长了状态太大，对状态加过期时间
 * val config=tabEnv.queryConfig.withIdleStateRetentionTime(Time.minutes(1),Time.minutes(6))
 * tabEnv.sqlUpdate('"',config)
 * tabEnv.sqlQuery("",config)
 * tab.writeToSink(sink,config)
 * 注意的是，这个时间的按照Processing-time来清理数据，意思和数据没关系，定时清理一下
 * 还有1、需要在每一个使用sqlUpdate/sqlQuery中单独设置
 *    2、定时清理也是flink的定时机制完成，注册的定时状态
 */
public class SQLMain {

    public static void main(String[] args) {


    }
}
//源码分析
//
//        Flink SQL 中使用了apache calcite来完成sql解析、验证、逻辑计划/物理计划生成以及优化工作，物理计划都需要实现DataStreamRel接口，其中DataStreamWindowJoin与DataStreamJoin 分别对应Time-window join 与 global window的物理执行计划，由于Time-window join 与 interval-join的实现步骤大体相似，最终还是会调用到IntervalJoinOperator,这里不做分析。主要分析一下，Global window 的执行过程，从DataStreamJoin入手。
//
//        DataStreamJoin中translateToPlan方法。
//        该方法获取左右两个流表对应的DataStream, 根据不同join 类型选择不同的ProcessFunction，例如inner join 选择NonWindowInnerJoin，将leftDataStream 与 rightDataStream 进行connect 得到ConnectedStreams 然后执行对应的ProcessFunction
//        以 inner join为例分析NonWindowInnerJoin, 继承了NonWindowJoin，而NonWindowJoin又继承了CoProcessFunction，与ProcessFunction针对一个流相反，CoProcessFunction是针对两个流的low level api, 可以访问状态、注册定时器。join 逻辑在其processElement方法中
//        override def processElement(
//        value: CRow,
//        ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
//        out: Collector[CRow],
//        timerState: ValueState[Long],
//        currentSideState: MapState[Row, JTuple2[Long, Long]],
//        otherSideState: MapState[Row, JTuple2[Long, Long]],
//        isLeft: Boolean): Unit = {
//
//        val inputRow = value.row
//        updateCurrentSide(value, ctx, timerState, currentSideState)
//
//        cRowWrapper.setCollector(out)
//        cRowWrapper.setChange(value.change)
//        val otherSideIterator = otherSideState.iterator()
//        // join other side data
//        while (otherSideIterator.hasNext) {
//        val otherSideEntry = otherSideIterator.next()
//        val otherSideRow = otherSideEntry.getKey
//        val otherSideCntAndExpiredTime = otherSideEntry.getValue
//        // join
//        cRowWrapper.setTimes(otherSideCntAndExpiredTime.f0)
//        callJoinFunction(inputRow, isLeft, otherSideRow, cRowWrapper)
//        // clear expired data. Note: clear after join to keep closer to the original semantics
//        if (stateCleaningEnabled && curProcessTime >= otherSideCntAndExpiredTime.f1) {
//        otherSideIterator.remove()
//        }
//        }
//        }
//        两个MapState对应两个流的缓存数据，key表示具体的数据ROW，Value表示数据ROW的数量与过期时间，由于数据流入过程中可能会存在多条相同的记录，以数据ROW作为key这种方式可以减少内存使用.
//        ValueState 用于存储数据的过期时间，以便任务失败恢复能够继续对数据执行过期操作。
//
//
//        processElement 执行流程：
//        a. updateCurrentSide 保存数据与更新数据的count与ttl, 同时会注册数据的过期时间，数据的过期时间是根据Idle State Retention Time来设置的，从StreamQueryConfig可以获取到
//        b. 循环遍历另外一个状态，调用callJoinFunction输出数据，在callJoinFunction里面使用的joinFunction是通过FunctionCodeGenerator动态生成的在，在DataStreamJoin的translateToPlan方法中被调用到，有兴趣可以debug 方式copy下来研读一下。
//
//        过期数据的清理定时是在updateCurrentSide注册的，其清理工作是在NonWindowJoin的onTimer方法完成，onTimer方法是从CoProcessFunction中继承过来的。在onTimer主要做过期时间判断并且清理。
