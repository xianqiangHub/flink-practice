package com.gusi.flink.analysis.orderpay;

/**
 * OrderLog <br>
 *
 */
public class OrderLog {
	private long orderId;
	private String type;
	private String txId;
	private long timestamp;

	public OrderLog(long orderId, String type, String txId, long timestamp) {
		this.orderId = orderId;
		this.type = type;
		this.txId = txId;
		this.timestamp = timestamp;
	}

	public long getOrderId() {
		return orderId;
	}

	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getTxId() {
		return txId;
	}

	public void setTxId(String txId) {
		this.txId = txId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "OrderLog{" +
				"orderId=" + orderId +
				", type='" + type + '\'' +
				", txId='" + txId + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
