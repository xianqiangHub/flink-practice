package com.bigdata.etl;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafakToHdfs {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60_000);
        env.setStateBackend((StateBackend) new FsStateBackend("/tmp/flink/checkpoints"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), props);
        DataStreamSource<String> stream = env.addSource(consumer);

        //StreamingFileSink 替代了先前的 BucketingSink
        StreamingFileSink<String> sink = StreamingFileSink
                /**
                 forRowFormat 表示输出的文件是按行存储的，对应的有 forBulkFormat，可以将输出结果用 Parquet 等格式进行压缩存储。
                 */
                .forRowFormat(new Path("/tmp/kafka-loader"), new SimpleStringEncoder<String>())
                .withBucketAssigner(new EventTimeBucketAssigner())
                .build();
        stream.addSink(sink);

        env.execute("KafakToHdfs");
    }

    public static class EventTimeBucketAssigner implements BucketAssigner<String, String> {
        @Override
        public String getBucketId(String element, Context context) {
//            JsonNode node = mapper.readTree(element);
//            long date = (long) (node.path("timestamp").floatValue() * 1000);
            long date = 1L; //json解析，从数据中获取timestamp
            String partitionValue = new SimpleDateFormat("yyyyMMdd").format(new Date(date));
            return "dt=" + partitionValue;
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return null;
        }
    }
}

/**
 * 完成一次性语义，首先端到端的数据源数据可重放，实现了checkpointfunction
 * flink内部的异步屏障快照，要开启checkpoint
 * sink端的StreamingFileSink，需要Hadoop2.7，先将文件写入缓冲文件，当最后一个快照完成，就是所有Barrie都到达
 * 到sink端，将缓冲文件写入
 * 类似于Kafkasink的两阶段提交
 *
 *程序运行过程中，StreamingFileSink 首先会将结果写入中间文件，以 . 开头、in-progress 结尾。这些中间文件会在符合一定条件后更名为正式文件，取决于用户配置的 RollingPolicy，默认策略是基于时间（60 秒）和基于大小（128 MB）。当脚本出错或重启时，中间文件会被直接关闭；在恢复时，由于检查点中保存了中间文件名和成功写入的长度，程序会重新打开这些文件，切割到指定长度（Truncate），然后继续写入。这样一来，文件中就不会包含检查点之后的记录了，从而实现 Exactly-once。

 以 Hadoop 文件系统举例，恢复的过程是在 HadoopRecoverableFsDataOutputStream 类的构造函数中进行的。它会接收一个 HadoopFsRecoverable 类型的结构，里面包含了中间文件的路径和长度。这个对象是 BucketState 的成员，会被保存在检查点中。
 HadoopRecoverableFsDataOutputStream(FileSystem fs, HadoopFsRecoverable recoverable) {
 this.tempFile = checkNotNull(recoverable.tempFile());
 truncate(fs, tempFile, recoverable.offset());
 out = fs.append(tempFile);
 }
 *
 */
