package com.gusi.flink.analysis.goodsStatistic;

/**
 * UserBehave <br>
 *
 */
public class UserBehave {
	private long userId;
	private long goodsId;
	private int kindId;
	private String behave;
	private long timestamp;

	public UserBehave(long userId, long goodsId, int kindId, String behave, long timestamp) {
		this.userId = userId;
		this.goodsId = goodsId;
		this.kindId = kindId;
		this.behave = behave;
		this.timestamp = timestamp;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public long getGoodsId() {
		return goodsId;
	}

	public void setGoodsId(long goodsId) {
		this.goodsId = goodsId;
	}

	public int getKindId() {
		return kindId;
	}

	public void setKindId(int kindId) {
		this.kindId = kindId;
	}

	public String getBehave() {
		return behave;
	}

	public void setBehave(String behave) {
		this.behave = behave;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
