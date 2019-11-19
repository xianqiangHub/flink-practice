package com.bigdata.topN;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Properties;
import java.util.TreeSet;

/**
 * 例：以热门销售商品为例，实时统计每10min内各个地域维度下销售额top10的商品。
 * **重要两点
 * 1.获取前10分钟窗口数据
 * 2.使用treeset排序
 */
public class WindowTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(5000L);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>("topic", new SimpleStringSchema(), props);
        DataStreamSource<String> stream = env.addSource(consumer);
        // 数据源类型是Kafka, 数据为订单数据包含：订单id、订单时间、商品id、区域id、订单金额（包含用户Id在这里省略）
        stream.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                String[] split = value.split(",");

                return new Order(split[0], Long.valueOf(split[1]), split[2], Double.valueOf(split[3]), split[4]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.orderTime;
            }
        }).keyBy(new KeySelector<Order, String>() {
            @Override
            public String getKey(Order value) throws Exception {
                return value.areaId + value.orderId; //以地区和订单进行分流
            }
        }).timeWindow(Time.minutes(10))
                .reduce(new ReduceFunction<Order>() {
                    @Override
                    public Order reduce(Order value1, Order value2) throws Exception {
                        return new Order(value1.orderId, value1.orderTime, value1.gdsId, value1.amount + value2.amount, value1.areaId);
                    }
                }) //求出了每个地区没每件商品每10钟的销售总额
                .keyBy(new KeySelector<Order, String>() {
                    @Override
                    public String getKey(Order value) throws Exception {
                        return value.areaId;
                    }
                }) //并按地区分流，下面第一个要做怎么获取上个10分钟窗口的数据
                //最简单的是重写设个同样大小的窗口，之所以可以这样简单理解上一个watermark触发窗口执行，输出了窗口的数据再发送的watermark
                //和hot-items的处理思路不一样
                .timeWindow(Time.minutes(10))
                .apply(new WindowFunction<Order, Order, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Order> input, Collector<Order> out) throws Exception {
                        //对销售总金额排序，这里主要是实现排序，使用的treeset
                        //
                        TreeSet<Order> orderTreeSet = new TreeSet<>(new Comparator<Order>() {
                            @Override
                            public int compare(Order o1, Order o2) {

                                return (int) (o1.amount - o2.amount);  //升序排列    ps： o2 - o1 降序
                            }
                        });
                        //默认取前10，可以写成参数
                        for (Order order : input) {
                            if (orderTreeSet.size() >= 10) {
                                Order min = orderTreeSet.first();
                                if (order.amount > min.amount) {
                                    orderTreeSet.pollFirst(); //升序，如果再过来的数据和第一个就是最小的比较，大于最小的就去掉最小的，将新到的加入
                                    orderTreeSet.add(order);
                                }
                            } else {
                                orderTreeSet.add(order);
                            }
                        }
                        //
                        for (Order order : orderTreeSet) {
                            out.collect(order);
                        }
                    }
                });
        env.execute("topN");
    }

    /**
     * 排序想到sorted的数据结构的treeSet(红黑树)和优先级队列proprityQueue(最大/最小堆)
     * 选择使用treeSet:
     * 1.flink的sql中的topN也是treemap
     * 2.不断的做数据插入那么就涉及不断的构造过程，相对而言选择红黑树比较好
     */
//    很容易想到的就是Sorted的数据结构TreeSet或者是优先级队列PriorityQueue , TreeSet 实现原理是红黑树，
// 优先队列实现原理就是最大/最小堆，这两个都可以满足需求，但是需要选择哪一个呢？红黑树的时间复杂度是logN，
// 而堆的构造复杂度是N, 读取复杂度是1， 但是我们这里需要不断的做数据插入那么就涉及不断的构造过程，相对而言选择红黑树比较
// (其实flink sql内部做topN也是选择红黑树类型的TreeMap)。
//
//    最后一点，是否需要保存所有的数据排序？很显然是不需要的，将TreeSet设置成为升序排序，那么第一个节点数据就是最小值，
// 当TreeSet里面的数据到达N, 就获取第一个节点数据(最小值)与当前需要插入的数据进行比较，如果比其大，则直接舍弃，如果比其小，
// 那么就将TreeSet中第一个节点数据删除，插入新的数据，最终得到的TreeSet 数据就是我们需要的topN。
    private static class Order {
        public String orderId;
        public Long orderTime;
        public String gdsId;
        public Double amount;
        public String areaId;

        public Order() {
        }

        public Order(String orderId, Long orderTime, String gdsId, Double amount, String areaId) {
            this.orderId = orderId;
            this.orderTime = orderTime;
            this.gdsId = gdsId;
            this.amount = amount;
            this.areaId = areaId;
        }
    }
}
