package com.inspur.test;

import redis.clients.jedis.Jedis;

import com.inspur.ratelimit.RateLimiterFactory;
import com.inspur.ratelimit.RedisUtil;

public class redisTest {
	public static void main(String[] args) {
		Jedis jedis = new Jedis("10.180.210.55");
		jedis.auth("1q2w3e4r!");
		System.out.println(jedis.ping());
		RateLimiterFactory rateLimiterFactory = new RateLimiterFactory();
		RedisUtil redis = new RedisUtil("10.180.210.55", 6379, "1q2w3e4r!");
		rateLimiterFactory.build("ratelimiter:im:msg", 600.0 / 30, 30, redis);
	}
}
