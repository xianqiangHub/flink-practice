package com.bigdata.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * keystate 每个key都有自己的state，互不相同，  key-group管理:
 * public void open(Configuration cfg) throws Exception {
 * count = getRuntimeContext().getState(new ValueStateDescriptor<>("myCount", Long.class));
 * }
 * <p>
 * operateState,并行度关联变化会重新分布 一般用于source:
 * ListCheckpointed
 * <p>
 * https://developer.aliyun.com/article/712711
 */
public class StateMain {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("docker", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(consumer);

        stream.keyBy("").map(new RichMapFunction<String, Object>() {

            @Override
            public void open(Configuration parameters) throws Exception {

//                getRuntimeContext().getState()
            }

            @Override
            public Object map(String value) throws Exception {
                return null;
            }
        });
    }
}

/**
 * Checkpoint 通过代码的实现方法如下：
 * <p>
 * 首先从作业的运行环境 env.enableCheckpointing 传入 1000，意思是做 2 个 Checkpoint 的事件间隔为 1 秒。Checkpoint 做的越频繁，恢复时追数据就会相对减少，同时 Checkpoint 相应的也会有一些 IO 消耗。
 * 接下来是设置 Checkpoint 的 model，即设置了 Exactly_Once 语义，并且需要 Barries 对齐，这样可以保证消息不会丢失也不会重复。
 * setMinPauseBetweenCheckpoints 是 2 个 Checkpoint 之间最少是要等 500ms，也就是刚做完一个 Checkpoint。比如某个 Checkpoint 做了700ms，按照原则过 300ms 应该是做下一个 Checkpoint，因为设置了 1000ms 做一次 Checkpoint 的，但是中间的等待时间比较短，不足 500ms 了，需要多等 200ms，因此以这样的方式防止 Checkpoint 太过于频繁而导致业务处理的速度下降。
 * setCheckpointTimeout 表示做 Checkpoint 多久超时，如果 Checkpoint 在 1min 之内尚未完成，说明 Checkpoint 超时失败。
 * setMaxConcurrentCheckpoints 表示同时有多少个 Checkpoint 在做快照，这个可以根据具体需求去做设置。
 * enableExternalizedCheckpoints 表示下 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点。
 */
