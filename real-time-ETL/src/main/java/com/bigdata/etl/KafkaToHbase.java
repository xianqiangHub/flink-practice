package com.bigdata.etl;

import com.bigdata.etl.utils.Constants;
import com.bigdata.etl.utils.HbaseUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 消费Kafka数据写入HBase时，单条处理效率太低。
 * 需要批量插入hbase,这里自定义时间窗口countWindowAll 实现100条hbase插入一次Hbase
 */
public class KafkaToHbase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //测试，就不做checkpoints了
//        env.enableCheckpointing(60_000);
//        env.setStateBackend((StateBackend) new FsStateBackend("/tmp/flink/checkpoints"));
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), props);
        DataStreamSource<String> stream = env.addSource(consumer);

        /*每10秒一个处理窗口*/
        DataStream<List<Put>> putList = stream.countWindowAll(Constants.windowCount).apply(new AllWindowFunction<String, List<Put>, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow window, Iterable<String> message, Collector<List<Put>> out) throws Exception {
                List<Put> putList = new ArrayList<Put>();
                for (String value : message) {
                    String rowKey = value.replace("::", "_");
                    Put put = new Put(Bytes.toBytes(rowKey.toString()));
                    String[] column = value.split("::");
                    for (int i = 0; i < column.length; i++) {
                        put.addColumn(Bytes.toBytes(Constants.columnFamily), Bytes.toBytes(Constants.columnArray.get(i)), Bytes.toBytes(column[i]));
                    }
                    putList.add(put);
                }
                out.collect(putList);
            }

        }).setParallelism(4);

        putList.addSink(new HBaseSinkFunction()).setParallelism(1);

        env.execute("KafkaToHbase");
    }

    //自定义hbase sink
    private static class HBaseSinkFunction extends RichSinkFunction<List<Put>> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            HbaseUtils.getConnection();
            TableName table = TableName.valueOf(Constants.tableNameStr);
            Admin admin = HbaseUtils.getConnection().getAdmin();
            if (!admin.tableExists(table)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Constants.tableNameStr));
                tableDescriptor.addFamily(new HColumnDescriptor(Constants.columnFamily));
                admin.createTable(tableDescriptor);
            }
        }

        @Override
        public void invoke(List<Put> putList, Context context) throws Exception {
            Table table = HbaseUtils.getConnection().getTable(TableName.valueOf(Constants.tableNameStr));
            table.put(putList);
        }

        @Override
        public void close() throws Exception {
            super.close();
            HbaseUtils.closeConn();
        }
    }
}
