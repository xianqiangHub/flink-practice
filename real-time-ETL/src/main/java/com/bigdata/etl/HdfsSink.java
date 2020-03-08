package com.bigdata.etl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
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

/**
 * https://mp.weixin.qq.com/s/1RgqRzZdd7RAuCndf4SwHg
 * StreamingFileSink压缩与合并小文件
 * <p>
 * 压缩：
 * <p>
 * 合并：
 * 众多的小文件会带来两个影响：
 * Hdfs NameNode维护元数据成本增加
 * 下游hive/spark任务执行的数据读取成本增加
 * 1、减少并行度
 * 2、增大checkpoint周期或者文件滚动周期
 * 3、下游再起一个MR或者Spark任务合并小文件
 */
public class HdfsSink {

    public static void main(String[] args) {

//        //行
//        StreamingFileSink.forRowFormat(new Path(path),
//                new SimpleStringEncoder<T>())
//                .withBucketAssigner(new PaulAssigner<>()) //分桶策略
//                .withRollingPolicy(new PaulRollingPolicy<>()) //滚动策略
//                .withBucketCheckInterval(CHECK_INTERVAL) //检查周期
//                .build();
//
////列 parquet
//        StreamingFileSink.forBulkFormat(new Path(path),
//                ParquetAvroWriters.forReflectRecord(clazz))
//                .withBucketAssigner(new PaulBucketAssigner<>())
//                .withBucketCheckInterval(CHECK_INTERVAL)
//                .build();
    }

}
