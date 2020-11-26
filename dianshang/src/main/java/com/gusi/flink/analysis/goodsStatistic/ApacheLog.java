package com.gusi.flink.analysis.goodsStatistic;


public class ApacheLog {
	private String ip;
	private long timestamp;
	private String method;
	private String url;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public String toString() {
		return "ApacheLog{" +
				"ip='" + ip + '\'' +
				", timestamp=" + timestamp +
				", method='" + method + '\'' +
				", url='" + url + '\'' +
				'}';
	}
}
