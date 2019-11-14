package com.bigdata.statemachine.kafka;

/**
 * 见broadcast-stream
 * 规则流
 * 广播状态因为descropter时mapstate
 * <p>
 * 广播状态是非key的，而rocksdb类型statebackend只能存储keyed状态类型，
 * 所以广播维表数据只能存储在内存中，因此在使用中需要注意维表的大小以免撑爆内存。
 */
public class KafkaRule {

//    val env=StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(60000)
//    val kafkaConfig = new Properties();
//    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
//    val ruleConsumer = new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), kafkaConfig)
//
//    val ruleStream=env.addSource(ruleConsumer)
//            .map(x=>{
//            val a=x.split(",")
//            Rule(a(0),a(1).toBoolean)
//    })
//    val broadcastStateDesc=new MapStateDescriptor[String,Rule]("broadcast-state",BasicTypeInfo.STRING_TYPE_INFO,TypeInformation.of(new TypeHint[Rule] {}))
//    val broadcastRuleStream=ruleStream.broadcast()
//
//    val userActionConsumer = new FlinkKafkaConsumer011[String]("topic2", new SimpleStringSchema(), kafkaConfig)
//
//    val userActionStream=env.addSource(userActionConsumer).map(x=>{
//            val a=x.split(",")
//            UserAction(a(0),a(1),a(2))
//    }).keyBy(_.userId)
//    val connectedStream=userActionStream.connect(broadcastRuleStream)
//
//    connectedStream.process(new KeyedBroadcastProcessFunction[String,UserAction,Rule,String] {
//
//        override def processElement(value: UserAction, ctx: KeyedBroadcastProcessFunction[String, UserAction, Rule, String]#ReadOnlyContext, out: Collector[String]): Unit = {
//
//                val state=ctx.getBroadcastState(broadcastStateDesc)
//        if(state.contains(value.actionType))
//        {
//            out.collect(Tuple4.apply(value.userId,value.actionType,value.time,"true").toString())
//        }
//      }
//        override def processBroadcastElement(value: Rule, ctx: KeyedBroadcastProcessFunction[String, UserAction, Rule, String]#Context, out: Collector[String]): Unit = {
//
//                ctx.getBroadcastState(broadcastStateDesc).put(value.actionType,value)
//        }
//    })
//
//            env.execute()
}
