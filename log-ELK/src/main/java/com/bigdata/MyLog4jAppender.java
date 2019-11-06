package com.bigdata;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * 自定义的appender
 */
public class MyLog4jAppender extends AppenderSkeleton {

    public String getDayin() {
        return dayin;
    }

    public void setDayin(String dayin) {
        this.dayin = dayin;
    }

    private String dayin;

    @Override
    protected void append(LoggingEvent event) {
        System.out.println("dayin" + event.getMessage());
    }

    @Override
    public void close() {

    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
