package com.bigdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * *****窗口的底层核心的类是 WindowOperator***************
 * Assigner、Trigger、Function、Evictor
 * <p>
 * countwindow:
 * 窗口要有assign和trigger 窗口函数
 * 窗口的中间数据是存储在状态中,
 * 一个operator状态的唯一性通过StateDesc、Key、Namespace, 窗口中namespace 就是window
 * 首先获取ReduceState 计数器，表示当前key 的数量，每处理一条数据就进行+1操作， 当count 达到执行触发量就会将当前key 计数state 清空，下次从0开始计数，并且触发窗口操作。
 * 这种状态计数器也会在checkpoint时候被储存，使其具有容错性，能够在任务失败中恢复，达到精确计数。
 * <p>
 * 1.窗口数据如何分配
 * Assigner：
 * 每个元素过来都会通过时间戳计算窗口的起始值加上size就是窗口大小，判断元素所属
 * 2.窗口数据触发前保存在哪
 * 窗口中每条数据都会调用    ，容错机制，对于keyStream，状态中保存的是key，当前的window，list窗口中value
 * <p>
 * 3、窗口数据如何触发的
 * 一般说窗口触发是watermark大于end-1,不严谨在有allowlateness存在的情况下，watermark触发窗口执行
 * 没有清空状态，再来的元素在allowlateness之前，会判断当前的watermark，直接触发
 * <p>
 * 4、窗口数据什么时候清空
 * 默认allowlateness为0，即窗口触发后清空状态，当allowlateness>0时，清空状态会重新注册timer为allowlateness时间
 * <p>
 * 5、延时窗口和延时数据的区别
 * 延迟窗口 islatewindow 过来的数据的看窗口是否还存在，一般都是
 * 延迟数据 islatedata  看数据是不是在watermark后面
 * <p>
 * 6、为什么聚合操作使用reduceFunction比wondowfunction好
 * 前者过来一条累加一条，后面是所有数据到齐再计算
 * <p>
 * 7、窗口allowedlateness对窗口的影响
 * 加上allowedlateness之后，在end-1 到 allowedlateness，之间的数据过来都会触发窗口，会多次输出，后面要能覆盖
 * <p>
 * <p>
 * ***************************
 * 一个元素 获取当前的watermark  此元素所属窗口的endtime
 */
public class SourceWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("docker", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(consumer);

        //侧输出的迟到数据 是end-1 + allowlateness
//        stream.keyBy("key").timeWindow().sideOutputLateData()
//        stream.keyBy("key").timeWindow().sum()


//        stream.map(new MapFunction<String, Tuple2<String,String>>() {
//            @Override
//            public Tuple2 map(String value) throws Exception {
//                return new Tuple2(value,"a");
//            }
//        })
////                keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(1),Time.days(1)))
////                timeWindow(Time.seconds(5)).max(1);


        env.execute("a");
    }
}
/**
 * flink window 实现机制分析
 * <p>
 * 原创： 阿甘  Flink 实战剖析  7月29日
 * 在flink streaming 处理中窗口是比较常见的操作, 例如窗口sum、max、min等，窗口构建主要包含：Assigner、Trigger、Function、Evictor, Assigner: 窗口分配器, 当有一个元素到达判断窗口属于哪一个窗口，对于滚动窗口分配给一个窗口, 对于滑动窗口可能会分配给多个窗口; Trigger: 窗口触发器, 决定什么时候触发窗口操作; Function: 窗口函数, 对窗口中数据执行的操作; Evictor: 窗口驱逐器, 并不常用，在Function 前后可以做一些额外的数据 处理工作。其整体工作流程就是：Assigner决定数据所属的窗口, 当满足一定条件Trigger触发窗口执行窗口Function, 在Function前后可以执行一些Evictor操作。
 * <p>
 * 接下来聚焦几个问题：
 * 1.  窗口数据如何分配
 * 2.  窗口数据在触发前保存在哪里
 * 3.  窗口操作是如何触发的
 * 4.  窗口数据什么时候清理
 * 5.  延时数据与延时窗口的区别
 * 6.  为什么聚合操作使用ReduceFunction 比WindowFunction 性能要好
 * 7.  窗口 allowedLateness 对窗口的影响
 * <p>
 * <p>
 * 窗口处理的流程都是围绕WindowOperator 这个类来执行，接下来将会根据源码来解析这几个问题。
 * <p>
 * <p>
 * 1. 窗口数据分配是由WindowAssigner来完成，常见按照时间进行分配，TumblingEventTimeWindows(滚动事件时间分配器)、SlidingEventTimeWindows(滑动事件时间分配器、TumblingProcessingTimeWindows(滚动处理时间分配器)、SlidingProcessingTimeWindows(滑动处理时间分配器),数据流入开始调用WindowAssigner.assignWindows 返回一个Window对象的集合，assignWindows会调用TimeWindow.getWindowStartWithOffset 计算所属窗口的startTime , 计算逻辑timestamp - (timestamp - offset + windowSize) % windowSize 这种计算方式默认情况会生成以0为startTime,的窗口, 如果有特殊需求例如需要-7.5-7.5、7.5-22.5这样的窗口, 那么可以设置offset偏移值, 相对于0的偏移, 对于此情况可以设置offset为7.5；
 * <p>
 * 2. 窗口数据如何保存：flink 是有状态的流处理, 其中间处理数据都会保存在 state中, 那么对于窗口数据也不例外, 在触发前都会保存在state 中, 保证了其容错机制, 对于每条数据的保存处理都会调用windowState.add(element.getValue()), 那么对于一个keyed window是如何区分各个窗口的数据的呢？在这里可以理解为有一个Map<Key,Window,List<Value>> 的数据结构其中key 表示具体的分组key值,Window 表示一个namespace 一个具体的window, List<Value>表示窗口中的数据;
 * <p>
 * 3. 窗口的触发：我们都知道当窗口的endTime 小于当前watermark 的时候就会触犯窗口操作, 但是这个说话其实并不严谨, 其实还有另外一种情况, 当到达的数据满足其所属的窗口的endTime 小于当前watermark时会触发窗口操作, 这两种说法类型但是分别对应两种触发机制：a . 基于watermark 的驱动触发 b.基于事件时间的触发, 以事件时间处理类型为例，在WindowOperator 处理数据的processElement 中会调用trigger.onElement方法，如果当前数据所属的窗口endTime 小于当前 watermark, 那么就会registerEventTimeTimer 注册一个事件时间的触发器, 会将当前的Window对象与endTime封装成为一个IntervalTimer放入一个优先级队列中（后续将会着重分享Flink定时系统）, 当处理watermark判断其值大于队列中endTime 的则触发窗口，这对应机制a,  机制b的触发就是在trigger.onElement中如果当前watermark大于窗口endTime则直接触发, 这种机制需要窗口allowedLateness >0;
 * <p>
 * 4. 窗口数据清理：窗口中间数据是保存在state中即内存中, 对于已经结束的窗口这部分数据已经是无效, 需要被清理掉, WindowOperator中在processElement中会调用registerCleanupTimer方法, 注册定时清理窗口数据,数据的清理时间是窗口的endTime+allowedLateness , allowedLateness 在事件时间处理中才有效, 此处注册的定时器就是生成一个IntervalTimer放入优先级队列中, 当到达窗口的watermark的大小大于endTime+allowedLateness就会在窗口函数执行之后触发清理操作, 默认allowedLateness=0, 也会简单认为窗口执行之后就会执行清理操作;
 * <p>
 * 5. 延时数据与延时窗口的区别：在WindowOperator中提供了两个方法isWindowLate与isElementLate, isWindowLate判断是否是延时窗口数据，依据是当该数据所属窗口的endTime小于当前的watermark就认为该数据应该被丢弃, isElementLate判断是否是延时数据, 依据是该数据时间小于当前的watermark，在窗口中可以设置lateDataOutputTag, 那么会将延时窗口数据放入该outputTag中，我们平时所属的处理窗口的延时数据真正意义上是处理延时窗口数据即满足isWindowLate的数据。需要注意在滑动窗口一个element可能属于多个窗口, 只要满足一个窗口是非延时的，那么就不会流入lateDataOutputTag中;
 * <p>
 * 6. ReduceFunction与WindowFunction的区别：在实际开发中经常有窗口聚合类的操作sum/min等, 按照一般的思考方式当窗口触发直接聚合窗口所有内的数据即可，即使用WindowFunction，但是这种方式会保存窗口所有的明细数据，对内存压力会比较大, 那么可不可以边接受数据边聚合数据呢，那么内存中对于一个窗口相同的key永远只保存一个聚合的值，可以使用ReduceFunction, ReduceFunction在其内部实现机制是定义了一个ReduceState,改state会使用ReduceFunction进行数据聚合, 当窗口触发是会执行PassThroughWindowFunction, 该Function仅仅是将窗口数据emit;
 * <p>
 * 7.  窗口allowedLateness : 设置了allowedLateness就相当于设置了窗口延时处理的二道防护线，在默认情况下当watermark大于窗口的endTime就会触发窗口操作并且执行窗口清理工作，但是当allowedLateness大于0，依据窗口清理规则watermark大于窗口endTime但是小于endTime+allowedLateness仅仅会触发窗口函数但是不会清理窗口，当后续后该窗口的数据到达那么会再次触发窗口操作，会造成两方面的影响 a: 内存消耗变大，窗口数据需要保留更长时间；b: 窗口数据的输出需要保持幂等性，即能够覆盖之前的输出结果，因为窗口函数会被多次触发。
 */