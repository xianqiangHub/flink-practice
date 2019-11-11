package com.bigdata.mysql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 这个例子是MySQL数据的定时全量加载，适合MySQL数据量小，变化慢
 * 代码实现解决：
 * 1、外部数据加载耗时，要异步加载
 * 2、还一个异步定时加载到内存的数据被流读取，是两个线程，可能有脏读，所以用AtomicReference，对象放到这里面
 * 3、加载到内存之后为同步的
 * <p>
 * 具体实例：广告流量统计
 * 广告流量数据包含：广告位id,用户设备id,事件类型(点击、浏览)，发生时间，
 * 现在需要统计每个广告主在每一个时间段内的点击、浏览数量，流量数据中只有广告位id,
 * 广告位id与广告主id对应的关系在mysql 中，这是一个典型的流表关联维表过程，需要从mysql中获取该广告位id对应的广告主id, 然后在来统计。
 */
public class MysqlMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("docker", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();

        DataStreamSource<String> source = env.addSource(consumer);
        source.map(new MapFunction<String, AdData>() {
            @Override
            public AdData map(String value) throws Exception {
                String[] split = value.split(",");

                return new AdData(0, Integer.valueOf(split[1]), split[2], Integer.valueOf(split[3]), Long.valueOf(split[4]));
            }
        }).flatMap(new MysqlFlatMapFunction()).print();

        env.execute("mysql");
    }
}

/**
 * 1、异步加载过程是异步线程执行，如果异步线程加载抛出异常是无法被Task检测，也就是无法导致任务失败，
 * 那么就会导致使用的维表数据一直都是变化之前的，对于业务来说是无法容忍的，
 * 解决方式:自定义一个维表关联的StreamOperator, 可获取到StreamTask， 然后再异步加载的异常处理中
 * 调用StreamTask.handleAsyncException方法，就可以导致任务失败，给用户发出警告
 * <p>
 * 2、维表全量加载是在每个task里面执行，那么就会导致每个task里面都有一份全量的维表数据，可采取优化方式是
 * 在维表关联前根据关联字段做keyBy操作，那么就会根据关联字段hash然后对并行度取余得到相同的值就会被分配到
 * 同一个task里面，所以在加载维表数据的时候也可以在每个task加载与其对应的维表数据， 就可以减少加载的数据量。
 * 其具体计算的规则是：
 * （MathUtils.murmurHash(key.hashCode()) % maxParallelism）*parallelism / maxParallelism
 * <p>
 * 得到的值就是IndexOfThisSubtask 即task的索引，那么可使用同样的算法过滤维表数据。
 */
