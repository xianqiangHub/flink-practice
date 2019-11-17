package com.bigdata.table;

import java.sql.Timestamp;

public class ClickLog {

    private String user;
    private Timestamp cTime;
    private String url;

    public ClickLog(String user, Timestamp cTime, String url) {
        this.user = user;
        this.cTime = cTime;
        this.url = url;
    }

    public ClickLog() {
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Timestamp getcTime() {
        return cTime;
    }

    public void setcTime(Timestamp cTime) {
        this.cTime = cTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ClickLog{" +
                "user='" + user + '\'' +
                ", cTime=" + cTime +
                ", url='" + url + '\'' +
                '}';
    }
}
