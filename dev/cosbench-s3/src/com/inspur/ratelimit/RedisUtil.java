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

	// Redis������IP
	private String ADDR = "10.180.210.55";

	// Redis�Ķ˿ں�
	private int PORT = 6379;

	// ��������
	private String AUTH = "1q2w3e4r!";

	// ��������ʵ���������Ŀ��Ĭ��ֵΪ8��
	// �����ֵΪ-1�����ʾ�����ƣ����pool�Ѿ�������maxActive��jedisʵ�������ʱpool��״̬Ϊexhausted(�ľ�)��
	private int MAX_ACTIVE = 1024;

	// ����һ��pool����ж��ٸ�״̬Ϊidle(���е�)��jedisʵ����Ĭ��ֵҲ��8��
	private int MAX_IDLE = 200;

	// �ȴ��������ӵ����ʱ�䣬��λ���룬Ĭ��ֵΪ-1����ʾ������ʱ����������ȴ�ʱ�䣬��ֱ���׳�JedisConnectionException��
	private int MAX_WAIT = 10000;

	private int TIMEOUT = 10000;

	// ��borrowһ��jedisʵ��ʱ���Ƿ���ǰ����validate���������Ϊtrue����õ���jedisʵ�����ǿ��õģ�
	private boolean TEST_ON_BORROW = true;

	private JedisPool jedisPool = null;
	private StringRedisTemplate redis = null;

	public RedisUtil(String addr, int port, String passwd) {
		this.ADDR = addr;
		this.PORT = port;
		this.AUTH = passwd;
	}
	/**
	 * ��ʼ��Redis���ӳ�
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
	 * ��ʼ��StringRedisTemplate���ӳ�
	 * @return 
	 */
	public StringRedisTemplate createStringRedisTemplate() {
		try {
			//ʵ�������ӹ���
			JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
			//����host
			connectionFactory.setHostName(this.ADDR);
			//���ö˿�
			connectionFactory.setPort(this.PORT);
			//��������
			connectionFactory.setPassword(this.AUTH);
			//��ʼ��connectionFactory
			connectionFactory.afterPropertiesSet();
			//ʵ����
		    this.redis = new StringRedisTemplate(connectionFactory);
			//��ʼ��StringRedisTemplate
			this.redis.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return this.redis;
	}
	/**
	 * ��ȡJedisʵ��
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
	 * �ͷ�jedis��Դ
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