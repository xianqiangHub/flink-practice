package com.gusi.flink.analysis.adclick;

/**
 * AdClick <br>
 *
 */
public class AdClick {
	private long userId;
	private long adId;
	private String province;
	private String city;
	private long timestamp;

	public AdClick() {
	}

	public AdClick(long userId, long adId, String province, String city, long timestamp) {
		this.userId = userId;
		this.adId = adId;
		this.province = province;
		this.city = city;
		this.timestamp = timestamp;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public long getAdId() {
		return adId;
	}

	public void setAdId(long adId) {
		this.adId = adId;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "AdClick{" +
				"userId=" + userId +
				", adId=" + adId +
				", province='" + province + '\'' +
				", city='" + city + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
