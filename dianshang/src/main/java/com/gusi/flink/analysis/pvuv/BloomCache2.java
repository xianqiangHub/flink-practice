package com.gusi.flink.analysis.pvuv;

/**
 * BloomCache2 <br>
 *
 */
public class BloomCache2 {

	private long cap = 1 << 29;

	public long hash(String value, int seed) {
		long result = 0;
		for (int i = 0; i < value.length(); i++) {
			// 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
			result = result * seed + value.charAt(i);
		}
		return (cap - 1) & result;
	}

}
