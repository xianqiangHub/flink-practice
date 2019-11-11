package com.bigdata.mysql;

public class AdData {

    private int aId;
    private int tId;
    private String clientId;
    private int actionType;
    private  Long time;

    public AdData() {
    }

    public AdData(int aId, int tId, String clientId, int actionType, Long time) {
        this.aId = aId;
        this.tId = tId;
        this.clientId = clientId;
        this.actionType = actionType;
        this.time = time;
    }

    public int getaId() {
        return aId;
    }

    public void setaId(int aId) {
        this.aId = aId;
    }

    public int gettId() {
        return tId;
    }

    public void settId(int tId) {
        this.tId = tId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getActionType() {
        return actionType;
    }

    public void setActionType(int actionType) {
        this.actionType = actionType;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }
}
