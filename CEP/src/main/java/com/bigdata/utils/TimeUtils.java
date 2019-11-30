package com.bigdata.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {

    public static void main(String[] args) throws ParseException {

        String time = stampToDate("1511658000000");
        System.out.println(time);//2017-11-26 09:00:00

        String data = dateToStamp("2017-11-26 09:05:00");
        System.out.println(data);//1511658300000

        String data2 = dateToStamp("2017-11-26 09:10:00");
        System.out.println(data2);//1511658600000

        String data1 = dateToStamp("2017-11-26 10:00:00");
        System.out.println(data1);//1511661600000

        String time3 = stampToDate("1511658317000");
        System.out.println(time3);
    }

    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    /*
    * 将时间转换为时间戳
    */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
}
