package com.gusi.flink.analysis.login;

/**
 * UserLoginLog <br>
 *
 */
public class UserLoginLog {
	private long userId;
	private String ip;
	private String result;
	private long timestamp;

	public UserLoginLog(long userId, String ip, String result, long timestamp) {
		this.userId = userId;
		this.ip = ip;
		this.result = result;
		this.timestamp = timestamp;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "UserLoginLog{" +
				"userId=" + userId +
				", ip='" + ip + '\'' +
				", result='" + result + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
