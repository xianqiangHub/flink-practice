package com.bigdata.topN;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Properties;
import java.util.TreeSet;

/**
 * 全局的topN比如用于大屏展示，和窗口topN区别是不是一个窗口的而是所有历史数据以来的
 * 注意：
 * 1、每个地区的每件商品的总金额在不停的变化
 * 2、大屏展示，为process time 并且 相同间隔定时触发
 * 实现：
 * 1、将topN放到valueState（treeSet结构，放的order对象）,mapState做判断数据重复到来关联（gdsId,order）
 * 首先判断如果valueState为空，初始化将来的第一条数据放到valueState，放入mapState
 * 之后每来一条数据，判断此商品是否存在topN中
 * 存在：直接更新（比他少的都已经在topN中）
 * 不存在：获取金额，商品数量达到N要做判断/没有达到N直接放进去
 * 核心：valueState放一个treeset,treeset放N个元素，取出N个元素是有序的
 * 2、相同间隔定时触发，触发后只返回放好的结果，类似场景都可以用
 */
public class GlobleTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
        }) //求销售总额
                .reduce(new ReduceFunction<Order>() {  //或者使用processfunction的reduceState
                    @Override
                    public Order reduce(Order value1, Order value2) throws Exception {
                        return new Order(value1.orderId, value1.orderTime, value1.gdsId, value1.amount + value2.amount, value1.areaId);
                    }
                }) //对地区和订单进行累加
                .keyBy(new KeySelector<Order, String>() {
                    @Override
                    public String getKey(Order value) throws Exception {
                        return value.areaId;
                    }
                }).process(new KeyedProcessFunction<String, Order, Order>() {

            private ValueStateDescriptor<TreeSet> topStateDesc;
            private ValueState<TreeSet> topState;
            private MapStateDescriptor<String, Order> mappingStateDesc;
            private MapState<String, Order> mappingState;
            private ValueStateDescriptor<Long> fireStateDesc;
            private ValueState<Long> fireState;

            Long interval = 1000L;
            int N = 10;

            @Override
            public void open(Configuration parameters) throws Exception {

                topStateDesc = new ValueStateDescriptor<>("top-state", TypeInformation.of(TreeSet.class));
                topState = getRuntimeContext().getState(topStateDesc);
                mappingStateDesc = new MapStateDescriptor<>("mapping-state", TypeInformation.of(String.class), TypeInformation.of(Order.class));
                mappingState = getRuntimeContext().getMapState(mappingStateDesc);
                fireStateDesc = new ValueStateDescriptor<>("fire-time", TypeInformation.of(Long.class));
                fireState = getRuntimeContext().getState(fireStateDesc);
            }

            @Override
            public void processElement(Order value, Context ctx, Collector<Order> out) throws Exception {

                TreeSet top = topState.value();
                if (top == null) {
                    //第一条数据过来初始化
                    TreeSet<Order> treeSet = new TreeSet<>(new Comparator<Order>() {
                        @Override
                        public int compare(Order o1, Order o2) {
                            return (int) (o1.amount - o2.amount);
                        }
                    });
                    treeSet.add(value); //添加第一条元素
                    topState.update(treeSet);
                    mappingState.put(value.gdsId, value); //商品和对象的映射，判断数据是否到过，能放到map的都是在topN的
                } else {

                    if (mappingState.contains(value.gdsId)) { //不是第一次来
                        Order oldV = mappingState.get(value.gdsId);
                        mappingState.put(value.gdsId, value);
                        TreeSet values = topState.value();
                        values.remove(oldV);
                        values.add(value);
                        topState.update(values);

                    } else {

                        if (top.size() >= N) { //达到N个判断
                            Double min = (Double) top.first();
                            if (value.amount > min) {
                                top.pollFirst();
                                top.add(value);
                                mappingState.put(value.gdsId, value);
                                topState.update(top);
                            }

                        } else { //没有打到N个直接放进去
                            top.add(value);
                            mappingState.put(value.gdsId, value);
                            topState.update(top);
                        }

                    }

                }

                //每个area ID注册一次，剩下timer触发再注册
                long currTime = ctx.timerService().currentProcessingTime();

                if (fireState.value() == null) {
                    long start = currTime - (currTime % interval);
                    long nextFireTimestamp = start + interval;
                    ctx.timerService().registerProcessingTimeTimer(nextFireTimestamp);
                    fireState.update(nextFireTimestamp);
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Order> out) throws Exception {
                for (Object o : topState.value()) {
                    out.collect((Order) o);
                }

                Long fireTimestamp = fireState.value(); //拿到的是上个元素触发，下一个定时的时间
                if (fireTimestamp != null && (fireTimestamp == timestamp)) {
                    fireState.clear();
                    fireState.update(timestamp + interval);
                    ctx.timerService().registerProcessingTimeTimer(timestamp + interval);
                }

            }
        }).print();

        env.execute("globleTopN");
    }

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
