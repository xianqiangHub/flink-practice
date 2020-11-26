package com.gusi.flink.analysis.pvuv;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * BloomCache <br>
 *
 */
public class BloomCache {

	private static BloomCache instance = new BloomCache();

	private BloomCache() {
	}

	public static BloomCache getInstance() {
		return instance;
	}

	//key:windowEndTime;value:countNumbers
	public static Map<Long, Long> countMap = new HashMap<>();


	private BloomFilter<String> bloom = BloomFilter.create(Funnels.stringFunnel(Charset.forName("utf-8")), 100000000, 0.0001);

	public static void main(String[] args) {
		BloomCache instance = BloomCache.getInstance();
		boolean f1 = instance.bloom.mightContain("abc");
		System.out.println(f1);
		instance.bloom.put("abc");

		boolean f2 = instance.bloom.mightContain("123");
		System.out.println(f2);
		instance.bloom.put("123");

		boolean f3 = instance.bloom.mightContain("abc");
		System.out.println(f3);
	}


	/**
	 * 包含返回true
	 *
	 * @param endTime
	 * @param value
	 * @return
	 */
	public boolean putValue(long endTime, String value) {
		boolean mightContain = bloom.mightContain(value);
		long count = 0;
		if (!mightContain) { //一定不包含，新数据
			boolean flag = bloom.put(value);
			if (flag) {
				if (countMap.get(endTime) == null) {
					count = 1;
					countMap.put(endTime, count);
				} else {
					count = countMap.get(endTime);
					countMap.put(endTime, ++count);
				}
			}

			return flag;
		}

		return false; //可能已经包含，重复数据
	}
}
