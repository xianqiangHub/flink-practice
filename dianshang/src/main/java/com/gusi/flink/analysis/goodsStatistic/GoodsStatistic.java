package com.gusi.flink.analysis.goodsStatistic;

public class GoodsStatistic {
	private long goodsId;
	private long count;
	private long startTime;
	private long endTime;

	public GoodsStatistic(long goodsId, long count, long startTime, long endTime) {
		this.goodsId = goodsId;
		this.count = count;
		this.startTime = startTime;
		this.endTime = endTime;
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

	public long getGoodsId() {
		return goodsId;
	}

	public void setGoodsId(long goodsId) {
		this.goodsId = goodsId;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "PvCount{" +
				"goodsId=" + goodsId +
				", count=" + count +
				'}';
	}
}
