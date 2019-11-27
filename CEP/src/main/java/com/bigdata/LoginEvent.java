package com.bigdata;

public class LoginEvent {
    String id;
    String ip;
    String status;
    Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(String id, String ip, String status, Long timestamp) {
        this.id = id;
        this.ip = ip;
        this.status = status;
        this.timestamp = timestamp;
    }
}
