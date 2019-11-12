package com.bigdata.hbase;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * LRU淘汰策略：当内存不足的时候淘汰最近最少访问的数据
 * 但是如果一些数据一直被方位就不会淘汰了，所以数据要加上过期时间，强制从外部重新加载
 * 第一种linkhashmap
 * hashmap是无序的，linkhashmap有序，从尾部插头部取，和插入顺序一致
 * LRU使用是固定长度的链表，从尾取从尾放，从头除去元素，访问过之后放到头部，存入的数据加上进入内存的时间
 * 获取的时候判断当前时间，ttl了，从外部再获取，放到尾部
 *
 * 第二种  Guava
 * 1、能配缓存大小
 * 2、过期时间
 * 3、淘汰策略  方便，实现了LRU淘汰策略
 */
public class Guava {
    /**
     * 最大缓存容量为1000，数据的过期时间为100s
     * @param args
     */
    public static void main(String[] args) {

        Cache<Object, Object> cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(100, TimeUnit.MILLISECONDS)
                .build();

    }
}
