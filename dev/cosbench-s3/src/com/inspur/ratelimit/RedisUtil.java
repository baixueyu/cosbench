package com.inspur.ratelimit;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

public final class RedisUtil {

	// Redis服务器IP
	private String ADDR = "10.180.210.55";

	// Redis的端口号
	private int PORT = 6379;

	// 访问密码
	private String AUTH = "1q2w3e4r!";

	// 可用连接实例的最大数目，默认值为8；
	// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
	private int MAX_ACTIVE = 1024;

	// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
	private int MAX_IDLE = 200;

	// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
	private int MAX_WAIT = 10000;

	private int TIMEOUT = 10000;

	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private boolean TEST_ON_BORROW = true;

	private JedisPool jedisPool = null;
	private StringRedisTemplate redis = null;

	public RedisUtil(String addr, int port, String passwd) {
		this.ADDR = addr;
		this.PORT = port;
		this.AUTH = passwd;
	}
	/**
	 * 初始化Redis连接池
	 * @return 
	 */
	public JedisPool CreateRedisPool() {
		try {
			JedisPoolConfig config = new JedisPoolConfig();
			//config.setMaxActive(MAX_ACTIVE);
			config.setMaxIdle(this.MAX_IDLE);
			//config.setMaxWait(MAX_WAIT);
			config.setMaxTotal(this.MAX_ACTIVE);
			config.setMaxWaitMillis(this.MAX_WAIT);
			config.setTestOnBorrow(this.TEST_ON_BORROW);
			this.jedisPool = new JedisPool(config, this.ADDR, this.PORT, this.TIMEOUT, this.AUTH);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return this.jedisPool;
	}

	/**
	 * 初始化StringRedisTemplate连接池
	 * @return 
	 */
	public StringRedisTemplate createStringRedisTemplate() {
		try {
			//实例化链接工厂
			JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
			//设置host
			connectionFactory.setHostName(this.ADDR);
			//设置端口
			connectionFactory.setPort(this.PORT);
			//设置密码
			connectionFactory.setPassword(this.AUTH);
			//初始化connectionFactory
			connectionFactory.afterPropertiesSet();
			//实例化
		    this.redis = new StringRedisTemplate(connectionFactory);
			//初始化StringRedisTemplate
			this.redis.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return this.redis;
	}
	/**
	 * 获取Jedis实例
	 * 
	 * @return
	 */
	public synchronized Jedis getJedis() {
		try {
			if (this.jedisPool != null) {
				Jedis resource = this.jedisPool.getResource();
				return resource;
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	public void returnResource(final Jedis jedis) {
		if (jedis != null) {
			this.jedisPool.returnResource(jedis);
		}
	}

	public static String toJson(Object obj) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(obj);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
		return null;

	}

	public static RedisPermits toPermits(String permits) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(permits, RedisPermits.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public StringRedisTemplate getRedis() {
		return this.redis;
	}

	public void setRedis(StringRedisTemplate redis) {
		this.redis = redis;
	}
	
}