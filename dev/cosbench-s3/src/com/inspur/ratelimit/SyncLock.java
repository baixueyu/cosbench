package com.inspur.ratelimit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.StringRedisTemplate;

class SyncLockFactory {
    private StringRedisTemplate stringRedisTemplate;
    private Map<String, SyncLock> syncLockMap = new HashMap<String, SyncLock>();  
    public SyncLockFactory() {
		super();
		// TODO Auto-generated constructor stub
	}  
	/**
     * 创建SyncLock
     *
     * @param key Redis key
     * @param expire Redis TTL/秒，默认10秒
     * @param safetyTime 安全时间/秒，为了防止程序异常导致死锁，在此时间后强制拿锁，默认 expire * 5 秒
	 * @param redis 
	 * @return 
     */
    SyncLock build(String key, Long expire, Long safetyTime, RedisUtil redis) {  
    	//不管存在否，均覆盖
        //if (!syncLockMap.containsKey(key)) {
        	//setStringRedisTemplate();
        this.stringRedisTemplate = redis.createStringRedisTemplate();
        syncLockMap.put(key, new SyncLock(key, stringRedisTemplate, expire, safetyTime));
        //}
        return syncLockMap.get(key);
    }
    
    public StringRedisTemplate getStringRedisTemplate() {
		return stringRedisTemplate;
	}

	public Map<String, SyncLock> getSyncLockMap() {
		return syncLockMap;
	}

	public void setSyncLockMap(Map<String, SyncLock> syncLockMap) {
		this.syncLockMap = syncLockMap;
	}
}

/**
 * 同步锁
 *
 * @property key  Redis key
 * @property stringRedisTemplate RedisTemplate
 * @property expire Redis TTL/秒
 * @property safetyTime 安全时间/秒
 * @constructor 请尽量避免直接构造该类，改用SyncLockFactory创建
 *
 * @see [SyncLockFactory]
 */
class SyncLock {
    private String key;
    private StringRedisTemplate stringRedisTemplate;
    private Long expire;
    private Long safetyTime;
    private Long waitMillisPer;
    private String value;
    
	public SyncLock(String key, StringRedisTemplate stringRedisTemplate,
			Long expire, Long safetyTime) {
		super();
		this.key = key;
		this.stringRedisTemplate = stringRedisTemplate;
		this.expire = expire;
		this.safetyTime = safetyTime;
		setWaitMillisPer();
		unLock();
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public StringRedisTemplate getStringRedisTemplate() {
		return stringRedisTemplate;
	}
	public void setStringRedisTemplate(StringRedisTemplate stringRedisTemplate) {
		this.stringRedisTemplate = stringRedisTemplate;
	}
	public Long getExpire() {
		return expire;
	}
	public void setExpire(Long expire) {
		this.expire = expire;
	}
	public Long getSafetyTime() {
		return safetyTime;
	}
	public void setSafetyTime(Long safetyTime) {
		this.safetyTime = safetyTime;
	}
	public Long getWaitMillisPer() {
		return waitMillisPer;
	}
	public void setWaitMillisPer() {
		this.waitMillisPer = (long) 10;
	}
	
	public String getValue() {
		this.value = "MultithreadLockValue";//Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
		return this.value;
	}
   
    /**
     * 尝试获取锁（立即返回）
     *
     * @return 是否获取成功
     *
     * @see [lock]
     * @see [unLock]
     */
	boolean tryLock() {
        boolean locked = stringRedisTemplate.opsForValue().setIfAbsent(key, getValue()) ? true : false;
        if (locked) {
            stringRedisTemplate.expire(key, expire, TimeUnit.SECONDS);
        }
        return locked;
    }

    /**
     * 尝试获取锁，并至多等待timeout时长
     *
     * @param timeout 超时时长
     * @param unit 时间单位
     *
     * @return 是否获取成功
     * @throws InterruptedException 
     *
     * @see [tryLock]
     * @see [lock]
     * @see [unLock]
     */
    boolean tryLock(Long timeout) throws InterruptedException {
    	Long waitMax = TimeUnit.MINUTES.toMillis(timeout);
        Long waitAlready = 0L;

        while (stringRedisTemplate.opsForValue().setIfAbsent(key, getValue()) != true && waitAlready < waitMax) {
            Thread.sleep(getWaitMillisPer());
            waitAlready += getWaitMillisPer();
        }

        if (waitAlready < waitMax) {
            stringRedisTemplate.expire(key, expire, TimeUnit.SECONDS);
            return true;
        }
        return false;
    }

    /**
     * 获取锁
     * @throws InterruptedException 
     *
     * @see [unLock]
     */
    void lock() throws InterruptedException {
        //String uuid = UUID.randomUUID().toString();
        Long waitMax = TimeUnit.SECONDS.toMillis(safetyTime);
        Long waitAlready = 0L;

        while (stringRedisTemplate.opsForValue().setIfAbsent(key, getValue()) != true && waitAlready < waitMax) {
            Thread.sleep(getWaitMillisPer());
            waitAlready += getWaitMillisPer();
        }
        // stringRedisTemplate.expire(key, expire, TimeUnit.SECONDS)
        stringRedisTemplate.opsForValue().set(key, value, expire, TimeUnit.SECONDS);
    }

    /**
     * 释放锁
     *
     * @see [lock]
     * @see [tryLock]
     */
    void unLock() {
       String iter =  stringRedisTemplate.opsForValue().get(getKey());
       if (iter != null && iter.equals(getValue())) {
           stringRedisTemplate.delete(key);
        }
    }
}