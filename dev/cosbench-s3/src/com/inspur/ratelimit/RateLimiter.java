package com.inspur.ratelimit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.Uninterruptibles;

public class RateLimiter {
	private String key;
	private Double permitsPerSecond;
	private int maxBurstSeconds = 60;

	private Long now;
	private RedisPermits permits;
	Jedis jedis;
	private final SleepingStopwatch stopwatch;

	RateLimiter(SleepingStopwatch stopwatch) {
		this.stopwatch = checkNotNull(stopwatch);
	}

	public RateLimiter(String key, Double permitsPerSecond, int maxBurstSeconds) {
		super();

		this.key = key;
		this.permitsPerSecond = permitsPerSecond;
		this.maxBurstSeconds = maxBurstSeconds;
		this.stopwatch = checkNotNull(SleepingStopwatch.createFromSystemTimer());
		this.jedis = RedisUtil.getJedis();
	}

	public Long getNow() {
		return System.currentTimeMillis();
	}

	public Jedis getJedis() {
		return jedis;
	}

	private RedisPermits putDefaultPermits() {
		permits = new RedisPermits(permitsPerSecond, maxBurstSeconds);

		this.jedis.set(key, permits.toString());
		return permits;
	}

	/**
	 * 获取value,即Redisermits对象
	 * 
	 * @return
	 */
	public RedisPermits getPermits() {

		if (RedisUtil.getJedis().get(key) == null) {
			String json = this.jedis.get(key);
			return permits;
		} else {
			return putDefaultPermits();
		}

	}

	/**
	 * 设置key的值
	 * 
	 * @param permits
	 */
	public void setPermits(RedisPermits permits) {
		this.jedis.set(key, permits);

	}

	// Can't be initialized in the constructor because mocks don't call the
	// constructor.
	private volatile Object mutexDoNotUseDirectly;

	private Object mutex() {
		Object mutex = mutexDoNotUseDirectly;
		if (mutex == null) {
			synchronized (this) {
				mutex = mutexDoNotUseDirectly;
				if (mutex == null) {
					mutexDoNotUseDirectly = mutex = new Object();
				}
			}
		}
		return mutex;
	}

	public double acquire() {
		return acquire(1);
	}

	public double acquire(int permits) {
		long microsToWait = reserve(permits);
		stopwatch.sleepMicrosUninterruptibly(microsToWait);
		return 1.0 * microsToWait / SECONDS.toMicros(1L);
	}

	final long reserve(int permits) {
		checkPermits(permits);
		synchronized (mutex()) {
			return reserveAndGetWaitLength(permits, stopwatch.readMicros());
		}
	}

	public boolean tryAcquire(long timeout, TimeUnit unit) {
		return tryAcquire(1, timeout, unit);
	}

	public boolean tryAcquire(int permits) {
		return tryAcquire(permits, 0, MICROSECONDS);
	}

	public boolean tryAcquire() {
		return tryAcquire(1, 0, MICROSECONDS);
	}

	public boolean tryAcquire(int token, long timeout, TimeUnit unit) {
		long timeoutMicros = max(unit.toMicros(timeout), 0);
		checkPermits(token);
		long microsToWait;
		synchronized (mutex()) {
			long nowMicros = stopwatch.readMicros();
			if (!canAcquire(nowMicros, timeoutMicros)) {
				return false;
			} else {
				microsToWait = reserveAndGetWaitLength(token, nowMicros);
			}
		}
		stopwatch.sleepMicrosUninterruptibly(microsToWait);
		return true;
	}

	private boolean canAcquire(long nowMicros, long timeoutMicros) {
		return queryEarliestAvailable(nowMicros) - timeoutMicros <= nowMicros;
	}

	private Long queryEarliestAvailable(Long tokens) {
		long n = now;
		RedisPermits permit = this.getPermits();
		permit.reSync(n);
		long storedPermitsToSpend = min(tokens, permit.storedPermits);
		long freshPermits = tokens - storedPermitsToSpend;
		long waitMillis = freshPermits * permit.intervalMillis; // 需要等待的时间
		return LongMath.checkedAdd(permit.nextFreeTicketMillis - n, waitMillis);
	}

	final long reserveAndGetWaitLength(int tokens, long nowMicros) {
		long n = now;
		RedisPermits permit = this.getPermits();
		permit.reSync(n);
		long storedPermitsToSpend = min(tokens, permit.storedPermits);
		long freshPermits = tokens - storedPermitsToSpend;
		long waitMillis = freshPermits * permit.intervalMillis; // 需要等待的时间
		permit.nextFreeTicketMillis = LongMath.checkedAdd(
				permit.nextFreeTicketMillis, waitMillis);
		permit.storedPermits -= storedPermitsToSpend;
		this.setPermits(permit);

		return permit.nextFreeTicketMillis - n;
	}

	abstract static class SleepingStopwatch {
		/*
		 * We always hold the mutex when calling this. TODO(cpovirk): Is that
		 * important? Perhaps we need to guarantee that each call to
		 * reserveEarliestAvailable, etc. sees a value >= the previous? Also, is
		 * it OK that we don't hold the mutex when sleeping?
		 */
		abstract long readMicros();

		abstract void sleepMicrosUninterruptibly(long micros);

		static final SleepingStopwatch createFromSystemTimer() {
			return new SleepingStopwatch() {
				final Stopwatch stopwatch = Stopwatch.createStarted();

				@Override
				long readMicros() {
					return stopwatch.elapsed(MICROSECONDS);
				}

				@Override
				void sleepMicrosUninterruptibly(long micros) {
					if (micros > 0) {
						Uninterruptibles.sleepUninterruptibly(micros,
								MICROSECONDS);
					}
				}
			};
		}
	}

	private static int checkPermits(int tokens) {
		// checkArgument(permits > 0, "Requested permits (%s) must be positive",
		// permits);
		Preconditions.checkArgument(tokens > 0,
				"Requested tokens $tokens must be positive");
		return tokens;
	}
}
