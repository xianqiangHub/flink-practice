package com.gusi.flink.analysis.pvuv;

/**
 * Count <br>
 *
 */
public class Count {
	private String type;
	private long value;

	public Count() {
	}


	public Count(String type, long value) {
		this.type = type;
		this.value = value;
	}


	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Count{" +
				"type='" + type + '\'' +
				", value=" + value +
				'}';
	}
}
