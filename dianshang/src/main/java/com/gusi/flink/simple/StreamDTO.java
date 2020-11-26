package com.gusi.flink.simple;


public class StreamDTO {
	private String dtoId;
	private double value;
	private long timestamp;

	public StreamDTO() {
	}

	public StreamDTO(String dtoId, double value, long timestamp) {
		this.dtoId = dtoId;
		this.value = value;
		this.timestamp = timestamp;
	}

	public String getDtoId() {
		return dtoId;
	}

	public void setDtoId(String dtoId) {
		this.dtoId = dtoId;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "StreamDto{" +
				"dtoId='" + dtoId + '\'' +
				", value=" + value +
				", timestamp=" + timestamp +
				'}';
	}
}
