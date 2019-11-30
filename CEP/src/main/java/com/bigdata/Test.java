package com.bigdata;

import com.bigdata.utils.TimeUtils;

//*******************************************************************
//        正常：流量在预设的正常范围内；
//        警告：某数据中心在10秒内连续两次上报的流量超过认定的正常值；
//        报警：某数据中心在30秒内连续两次匹配警告；
public class Test {

    public static void main(String[] args) {

        System.out.println(TimeUtils.stampToDate("1558430842"));
        System.out.println(TimeUtils.stampToDate("1558430842"));
        System.out.println(TimeUtils.stampToDate("1558430842"));
        System.out.println(TimeUtils.stampToDate("1558430842"));
    }


}
