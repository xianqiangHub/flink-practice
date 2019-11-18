package com.bigdata.UV;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;


public class UVWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {

    private ValueState<Long> uvState;
    private MapState<String, Boolean> userMapState;
    private StateTtlConfig ttlConfig;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ttlConfig = StateTtlConfig
                .newBuilder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .cleanupInRocksdbCompactFilter(1000L)
                .build();
    }

    @Override
    public void process(String url, Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
        //因为是统计每天的uv，所以用窗口时间的日期作为state name的一部分，保证同一天的数据使用同一个state，不同天的数据使用不同的state
        long startTime = context.window().getStart();
        String day = new SimpleDateFormat("yyyyMMdd").format(new Date(startTime));

        ValueStateDescriptor<Long> uvDes = new ValueStateDescriptor(day + "uv", Long.class);
        MapStateDescriptor<String, Boolean> userMapDes = new MapStateDescriptor(day + "userMap", String.class, Boolean.class);

        //设置state的过期时间
        uvDes.enableTimeToLive(ttlConfig);
        userMapDes.enableTimeToLive(ttlConfig);

        userMapState = getRuntimeContext().getMapState(userMapDes);
        uvState = getRuntimeContext().getState(uvDes);
//        this.uvState = context.globalState().getState(uvDes);
//        userMapState = context.globalState().getMapState(userMapDes);

        if (uvState.value() == null) {//init uv
            uvState.update(0L);
        }

        long uv = uvState.value();
        for (String log : iterable) {
            String tokens[] = log.split(UVStats.DELIM, -1);
            String userId = tokens[1];
            //从这块去重，判断用户是否已存在   ？？是否有更好的方法
            if (!userMapState.contains(userId)) {
                userMapState.put(userId, true);
                uv++;
            }
        }
        uvState.update(uv);

        collector.collect(day + "\t" + url + "\t" + uv);
    }

}
