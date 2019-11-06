package com.bigdata.exercise;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 需求：
 * 将postgresql中的数据读取到streamPgSql中，作为配置数据，包含code和name
 * 同时将streamPgSql通过广播，减少数据的内存消耗
 *
 * 将kafka中的数据与postgresql中的数据进行join，清洗，得到相应的数据
 *
 * Broadcast会将state广播到每个task
 * 注意该state并不会跨task传播
 * 对其修改，仅仅是作用在其所在的task
 */
public class MyTest {
    public static void main(String[] args) throws Exception {
        final String zookeeper = "";
        final String topic = "web";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(5000);  //检查点 每5000ms
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        final StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", zookeeper);//zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
        properties.setProperty("group.id", "flinkStream");//flink consumer flink的消费者的group.id

        //1、读取postgresQL的配置消息
        DataStream<String> streamPgSql = env.addSource(new PostgresqlSource());

        final DataStream<HashMap<String, String>> conf = streamPgSql.map(new MapFunction<String, HashMap<String, String>>() {
            @Override
            public HashMap<String, String> map(String value) throws Exception {
                String[] tokens = value.split("\\t");
                HashMap<String, String> hashMap = new HashMap<>();
                hashMap.put(tokens[0], tokens[1]);
                System.out.println(tokens[0] + " : " + tokens[1]);
                return hashMap;
//                return new Tuple2<>(tokens[0],tokens[1]);
            }
        });


        //2、创建MapStateDescriptor规则，对广播的数据的数据类型的规则
        MapStateDescriptor<String, Map<String, String>> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState"
                , BasicTypeInfo.STRING_TYPE_INFO
                , new MapTypeInfo<>(String.class, String.class));//Map的TypeInformation
        //3、对conf进行broadcast返回BroadcastStream  规则
        BroadcastStream<HashMap<String, String>> confBroadcast = conf.broadcast(ruleStateDescriptor);

        //读取kafka中的stream
        FlinkKafkaConsumer<String> webStream = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        webStream.setStartFromEarliest();
        DataStream<String> kafkaData = env.addSource(webStream).setParallelism(1);
        //192.168.108.209	2019-05-07 16:11:09	"GET /class/2.html"	503	https://search.yahoo.com/search?p=java核心编程
        DataStream<Tuple5<String, String, String, String, String>> map = kafkaData.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(String value) throws Exception {
                String[] tokens = value.split("\\t");
                return new Tuple5<>(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4]);
            }
        })
                //使用connect连接BroadcastStream，然后使用process对BroadcastConnectedStream流进行处理
                .connect(confBroadcast)
                .process(new BroadcastProcessFunction<Tuple5<String, String, String, String, String>, HashMap<String, String>, Tuple5<String, String, String, String, String>>() {
                    private HashMap<String, String> keyWords = new HashMap<>();
                    //定义一样的，通过这个state获取广播的state
                    MapStateDescriptor<String, Map<String, String>> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState"
                            , BasicTypeInfo.STRING_TYPE_INFO
                            , new MapTypeInfo<>(String.class, String.class));

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(Tuple5<String, String, String, String, String> value, ReadOnlyContext ctx, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
//                        Thread.sleep(10000);
                        Map<String, String> map = ctx.getBroadcastState(ruleStateDescriptor).get("keyWords");
                        String result = map.get(value.f3);
                        if (result == null) {
                            out.collect(new Tuple5<>(value.f0, value.f1, value.f2, value.f3, value.f4));
                        } else {
                            out.collect(new Tuple5<>(value.f0, value.f1, value.f2, result, value.f4));
                        }

                    }

                    /**
                     * 接收广播中的数据
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(HashMap<String, String> value, Context ctx, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
//                        System.out.println("收到广播数据："+value.values());
                        BroadcastState<String, Map<String, String>> broadcastState = ctx.getBroadcastState(ruleStateDescriptor);
                        keyWords.putAll(value);
                        //注意该state并不会跨task传播    对其修改，仅仅是作用在其所在的task
                        broadcastState.put("keyWords", keyWords);
                    }
                });

        map.print();
        env.execute("Broadcast test kafka");
    }
}
