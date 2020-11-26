package com.gusi.flink.analysis.goodsStatistic;

/**
 * UrlCount <br>
 *
 */
public class UrlStatistic {
	private String url;
	private long count;
	private long startTime;
	private long endTime;

	public UrlStatistic(String url, long count, long startTime, long endTime) {
		this.url = url;
		this.count = count;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	@Override
	public String toString() {
		return "UrlCount{" +
				"url='" + url + '\'' +
				", count=" + count +
				", startTime=" + startTime +
				", endTime=" + endTime +
				'}';
	}
}
