package com.gusi.flink.analysis.orderpay;

/**
 * PayedRsp <br>
 *
 */
public class PayedRsp {
	private String txId;
	private String channel;
	private long timestamp;

	public PayedRsp(String txId, String channel, long timestamp) {
		this.txId = txId;
		this.channel = channel;
		this.timestamp = timestamp;
	}

	public String getTxId() {
		return txId;
	}

	public void setTxId(String txId) {
		this.txId = txId;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "PayedRsp{" +
				"txId='" + txId + '\'' +
				", channel='" + channel + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
