package com.bigdata.demoFour;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.HashMap;
import java.util.Map;

public class MySource extends RichSourceFunction<String> {

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        Map<String, String> map = new HashMap<>();

//        map.put("userid", "1");
//        map.put("orderid", "2222");
//        map.put("behave", "order");
//        ctx.collect(JSON.toJSONString(map));
//
//        Thread.sleep(1000);
//
//        map.put("userid", "1");
//        map.put("orderid", "2222");
//        map.put("behave", "pay");
//        ctx.collect(JSON.toJSONString(map));
//        Thread.sleep(1000);

        //______________________________________________________

        map.put("userid", "2");
        map.put("orderid", "2223");
        map.put("behave", "pay");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(1000);

        map.put("userid", "2");
        map.put("orderid", "2224");
        map.put("behave", "order");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(4000);

        map.put("userid", "2");
        map.put("orderid", "2224");
        map.put("behave", "pay");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(4000);

        map.put("userid", "2");
        map.put("orderid", "2223");
        map.put("behave", "order");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(1000);

        map.put("userid", "2");
        map.put("orderid", "2223");
        map.put("behave", "order");
        ctx.collect(JSON.toJSONString(map));
//        Thread.sleep(1000);
    }

    @Override
    public void cancel() {

    }
}
