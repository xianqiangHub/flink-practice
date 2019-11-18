package com.bigdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 计算PV UV 窗口比较大，一般会用到窗口内的循环触发
 * <p>
 * 这个例子是每个区域的一小时内的销售额，需要每一分钟显示一次
 * 从源码可以看出每一个元素首先判断 窗口的最大时间戳是否小于当前watermark，小于直接触发
 * 触发之后执行onEventTime，首先判断是否是窗口触发的，窗口触发的清理所有状态
 * 是中间定时触发的，会执行并且再注册下一个间隔的定时器
 * ***
 * 1、连续定时触发与第一条数据有关
 * long start = timestamp - (timestamp % interval);
 * long nextFireTimestamp = start + interval;
 * 2、如果数据时间间隔相对于定期触发的interval比较大，那么有可能会存在多次输出相同结果的场景，
 * 比喻说触发的interval是10s, 第一条数据时间是2019-11-16 11:22:00, 那么下一次的触发时间是2019-11-16 11:22:10，
 * 如果此时来了一条2019-11-16 11:23:00 的数据，会导致其watermark直接提升了1min, 会直接触发5次连续输出，对于下游处理来说可能会需要做额外的操作。
 *      因为每次执行都会重新注册个定时器，下一条数据间隔过大，会触发多次
 * 3、窗口的每一个key的触发时间可能会不一致，是因为窗口的每一个key对应的第一条数据时间不一样，正如上述所描述定时规则。
 * 由于会注册一个窗口endTime的触发器，会触发窗口所有key的窗口函数，保证最终结果的正确性。
 */
public class ContinuousEventTimeTriggerDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), props);
        DataStreamSource<String> stream = env.addSource(consumer);
        //************************
        //orderId03,1573874530000,gdsId03,300,beijing  (2019-11-16 11:22:10,下一个触发时间是2019-11-16 11:23:00)
        //orderId03,1573874740000,gdsId03,300,hangzhou  (2019-11-16 11:25:40,下一个触发时间是2019-11-16 11:26:00)
        KeyedStream<AreaOrder, String> keyedStream = stream.map(new MapFunction<String, Order>() {
            //<T, KEY>
            @Override
            public Order map(String value) throws Exception {
                String[] split = value.split(",");
                return new Order(split[0], Long.valueOf(split[1]), split[2], Double.valueOf(split[3]), split[4]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(30)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.orderTime;
            }
        }).map(new MapFunction<Order, AreaOrder>() {
            @Override
            public AreaOrder map(Order value) throws Exception {

                return new AreaOrder(value.orderId, value.amount);
            }
        }).keyBy(new KeySelector<AreaOrder, String>() {
            @Override
            public String getKey(AreaOrder value) throws Exception {
                return value.areaId;
            }
        });
        //key为每个区域的id,对象中包括金额，对金额做累加
        keyedStream.timeWindow(Time.hours(1))
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))
                .reduce(new ReduceFunction<AreaOrder>() {
                    @Override
                    public AreaOrder reduce(AreaOrder value1, AreaOrder value2) throws Exception {

                        return new AreaOrder(value1.areaId, value1.amount + value2.amount);
                    }
                }).print();


        env.execute("start");
    }


    private static class AreaOrder {
        public String areaId;
        public Double amount;

        public AreaOrder(String areaId, Double amount) {
            this.areaId = areaId;
            this.amount = amount;
        }

        public AreaOrder() {
        }
    }

    private static class Order {
        public String orderId;
        public Long orderTime;
        public String gdsId;
        public Double amount;
        public String areaId;

        public Order(String s, Long aLong, String s1, Double aDouble, String s2) {

        }
    }
}
