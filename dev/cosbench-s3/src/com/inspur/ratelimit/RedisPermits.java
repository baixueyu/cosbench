package com.inspur.ratelimit;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class RedisPermits {

	long maxPermits;
	long storedPermits = (long) 60;
	long intervalMillis;
	long nextFreeTicketMillis = System.currentTimeMillis();

	ObjectMapper mapper = new ObjectMapper();

	public RedisPermits(double permitsPerSecond, int maxBurstSeconds) {
		this.maxPermits = (long) (permitsPerSecond * maxBurstSeconds);
		this.storedPermits = (long) permitsPerSecond;
		long time = TimeUnit.SECONDS.toMillis(1);
		this.intervalMillis = (long) (TimeUnit.SECONDS.toMillis(1) / permitsPerSecond);
		this.nextFreeTicketMillis = System.currentTimeMillis();

	}

	public RedisPermits() {

	}

	public RedisPermits(long maxPermits, long storedPermits,
			long intervalMillis, long nextFreeTicketMillis) {
		super();
		this.maxPermits = maxPermits;
		this.storedPermits = storedPermits;
		this.intervalMillis = intervalMillis;
		this.nextFreeTicketMillis = nextFreeTicketMillis;
	}

	/**
	 * 计算redis-key过期时长（秒）
	 * 
	 * @return redis-key过期时长（秒）
	 */
	public Long expires() {
		Long now = System.currentTimeMillis();
		return 2
				* TimeUnit.MINUTES.toSeconds(1)
				+ TimeUnit.MILLISECONDS
						.toSeconds(max(nextFreeTicketMillis, now) - now);
	}

	/**
	 * if nextFreeTicket is in the past, reSync to now
	 * 若当前时间晚于nextFreeTicketMicros，则计算该段时间内可以生成多少令牌，将生成的令牌加入令牌桶中并更新数据
	 * 
	 * @return 是否更新
	 */
	public boolean reSync(Long now) {
		if (now > nextFreeTicketMillis) {
			storedPermits = min(maxPermits, storedPermits
					+ (now - nextFreeTicketMillis) / intervalMillis);

			nextFreeTicketMillis = now;
			return true;
		}
		return false;
	}

	public long getMaxPermits() {
		return maxPermits;
	}

	public void setMaxPermits(long maxPermits) {
		this.maxPermits = maxPermits;
	}

	public long getStoredPermits() {
		return storedPermits;
	}

	public void setStoredPermits(long storedPermits) {
		this.storedPermits = storedPermits;
	}

	public long getIntervalMillis() {
		return intervalMillis;
	}

	public void setIntervalMillis(long intervalMillis) {
		this.intervalMillis = intervalMillis;
	}

	public long getNextFreeTicketMillis() {
		return nextFreeTicketMillis;
	}

	public void setNextFreeTicketMillis(long nextFreeTicketMillis) {
		this.nextFreeTicketMillis = nextFreeTicketMillis;
	}

	@Override
	public String toString() {
		try {
			return mapper.writeValueAsString(this);
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

	public static void main(String[] args) {
		RedisPermits permit = new RedisPermits(1, 1);

		ObjectMapper mapper = new ObjectMapper();

		try {
			String json = mapper.writeValueAsString(permit);
			System.out.println(json);

			// mapper.configure(
			// DeserializationConfig.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
			// true);
			RedisPermits p = mapper.readValue(json, RedisPermits.class);
			System.out.println(p);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
