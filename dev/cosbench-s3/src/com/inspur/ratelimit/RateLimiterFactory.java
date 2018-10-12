package com.inspur.ratelimit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import redis.clients.jedis.Jedis;

public class RateLimiterFactory {

	private Jedis jedis = new Jedis("10.180.210.55");
	private ReentrantLock lock = new ReentrantLock();

	private Map rateLimiterMap = new HashMap<String, RateLimiter>();

	/**
	 * 创建RateLimiter
	 * 
	 * @param key
	 *            Redis key
	 * @param permitsPerSecond
	 *            每秒放入的令牌数
	 * @param maxBurstSeconds
	 *            最大存储maxBurstSeconds秒生成的令牌
	 * 
	 * @return RateLimiter
	 */

	public synchronized RateLimiter build(String key, Double permitsPerSecond,
			int maxBurstSeconds) {
		if (!rateLimiterMap.containsKey(key)) {
			rateLimiterMap.put(key, new RateLimiter(key, permitsPerSecond,
					maxBurstSeconds));
		}
		return (RateLimiter) rateLimiterMap.get(key);
	}

}
