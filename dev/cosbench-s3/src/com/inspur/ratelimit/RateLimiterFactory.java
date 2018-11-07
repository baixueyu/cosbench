package com.inspur.ratelimit;

import java.util.HashMap;

public class RateLimiterFactory {

	// private Jedis jedis = new Jedis("10.180.210.55");
	// private ReentrantLock lock = new ReentrantLock();
	private SyncLockFactory syncLockFactory = new SyncLockFactory();

	private HashMap<String, RateLimiter> rateLimiterMap = new HashMap<String, RateLimiter>();

	/**
	 * ����RateLimiter
	 * 
	 * @param key
	 *            Redis key
	 * @param permitsPerSecond
	 *            ÿ������������
	 * @param maxBurstSeconds
	 *            ���洢maxBurstSeconds�����ɵ�����
	 * 
	 * @return RateLimiter
	 */

	public synchronized RateLimiter build(String key, Double permitsPerSecond,
			int maxBurstSeconds) {
		//if (!rateLimiterMap.containsKey(key)) {
		//�����Ƿ���ڣ�������
		rateLimiterMap.put(key, new RateLimiter(key, permitsPerSecond,
					maxBurstSeconds, syncLockFactory.build(key + ":lock",  10L,  50L)));
		//}
		return (RateLimiter) rateLimiterMap.get(key);
	}

}
