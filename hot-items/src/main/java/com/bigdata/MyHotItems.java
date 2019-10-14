package com.bigdata;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * 通过本文你将学到：
 * <p>
 * 如何基于 EventTime 处理，如何指定 Watermark
 * 如何使用 Flink 灵活的 Window API
 * 何时需要用到 State，以及如何使用
 * 如何使用 ProcessFunction 实现 TopN 功能
 * <p>
 * 抽取出业务时间戳，告诉 Flink 框架基于业务时间做窗口
 * 过滤出点击行为数据
 * 按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
 * 按每个窗口聚合，输出每个窗口中点击量前N名的商品
 */

public class MyHotItems {

    public static void main(String[] args) throws Exception {
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(300000);

//        // UserBehavior.csv 的本地文件路径, 在 resources 目录下
//        URL fileUrl = MyHotItems.class.getClassLoader().getResource("MyUserBehavior.csv");
//        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
//        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
//        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
//        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
//        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
//        // 创建 PojoCsvInputFormat
//        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
//
//        // 创建数据源，得到 UserBehavior 类型的 DataStream
//        DataStreamSource<UserBehavior> source = env.createInput(csvInput, pojoType);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        FlinkKafkaConsumer<UserBehavior> fc = new FlinkKafkaConsumer<>("hotitem", new MyUserBehavior(), prop);
        fc.setStartFromLatest();
        DataStreamSource<UserBehavior> source = env.addSource(fc);

        // 抽取出时间和生成 watermark
//        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
//            @Override
//            public long extractAscendingTimestamp(UserBehavior userBehavior) {
//                // 原始数据单位秒，将其转成毫秒
//                return userBehavior.getTimestamp() * 1000;
//            }
//        });
        //乱序的测试，定期生成watermark
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000; //fen pei chuang kou
            }
        });
        userBehaviorSingleOutputStreamOperator
                // 过滤出只有点击的数据
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        // 过滤出只有点击的数据
                        return userBehavior.getBehavior().equals("pv");
                    }
                })
                .keyBy("itemId")
                .timeWindow(Time.minutes(60))
                //用到trigger，当窗口比较大，中间要触发执行
                //第5秒的数据是包括在触发中的
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                //驱逐元素，要保证在下面aggregate保存了中间状态
                //每次trriger保存下中间状态，驱逐已经计算的元素
                //evictBefore()：移除窗口元素，在Window Function之前调用。
               //evictAfter()：移除窗口元素，在Window Function之后调用。
//                .evictor(TimeEvictor.of(Time.seconds(0),true))//计算完丢掉
                //这个是每次触发计算只计算  最近的时间的数据
                //注意的是  watermark触发，为最后一条数据的时间戳去减 这个5秒
                .evictor(TimeEvictor.of(Time.seconds(5)))//计算前丢掉 默认 false
                .aggregate(new CountAgg(),new MyWindowResultFunction())
                .print();

//                .aggregate(new CountAgg(), new WindowResultFunction()) //aggregate 相对于apply，会做预聚合 增量聚合， WindowResultFunction作用于窗口的函数，决定怎么输出，触发窗口发出元素，到这个函数中
//                .keyBy("windowEnd")
//                .process(new TopNHotItems(3))
//                .print();

        env.execute("Hot Items Job");
    }

    /**
     * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
     */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(
                ItemViewCount input,
                Context context,
                Collector<String> collector) throws Exception {

            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < allItems.size() && i < topSize; i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("====================================\n\n");

            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);

            out.collect(result.toString());
        }
    }

    /**
     * 用于输出窗口的结果
     */
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /**
     * COUNT 统计的聚合函数实现，每出现一条记录加一
     */
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    public static class MyWindowResultFunction implements WindowFunction<Long, Tuple2<Long, Long>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

            Long Mykey = ((Tuple1<Long>) key).f0;
            Long next = input.iterator().next();
            out.collect(new Tuple2(Mykey,next));
        }
    }

    /**
     * 商品点击量(窗口操作的输出类型)
     */
    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

//    /**
//     * 用户行为数据结构
//     **/
//    public static class UserBehavior {
//        public long userId;         // 用户ID
//        public long itemId;         // 商品ID
//        public int categoryId;      // 商品类目ID
//        public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav") 浏览 购买 加购 收藏
//        public long timestamp;      // 行为发生的时间戳，单位秒
//    }
}
