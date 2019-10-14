package com.bigdata;

public class UserBehavior {
    private long userId;         // 用户ID
    private long itemId;         // 商品ID
    private int categoryId;      // 商品类目ID

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    private String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav") 浏览 购买 加购 收藏
    private long timestamp;      // 行为发生的时间戳，单位秒

    @Override
    public String toString() {
        return userId + "," + itemId + "," + categoryId + "," + behavior + "," + timestamp;
    }
}
