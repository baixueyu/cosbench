package com.inspur.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.inspur.ratelimit.RateLimiter;
import com.inspur.ratelimit.RateLimiterFactory;
import com.inspur.ratelimit.RedisUtil;

public class QosMThread {
	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// RateLimiter limiter = RateLimiter.create(20.0);// 每秒放置v个令脾
		RateLimiterFactory rateLimiterFactory = new RateLimiterFactory();
		RedisUtil redis = new RedisUtil("10.180.210.55", 6379, "1q2w3e4r!");
		RateLimiter limiter = rateLimiterFactory.build("ratelimiter:im:msg2",
				10., 30, redis);
		
		long count = 0;
		Date start = new Date();
		Long s = System.nanoTime();
		System.out.println(sdf.format(start));
		
		//模拟多线程Qos
		WorkThread myThread = new WorkThread();
		myThread.setRatelimit(limiter);
		myThread.setCount(count);
		
		WorkThread myThread1 = new WorkThread();
		myThread1.setRatelimit(limiter);
		myThread1.setCount(count);
		
		WorkThread myThread2 = new WorkThread();
		myThread2.setRatelimit(limiter);
		myThread2.setCount(count);
		
    	Thread thread = new Thread(myThread);
    	Thread thread1 = new Thread(myThread1);
       	Thread thread2 = new Thread(myThread2);
       	
    	thread.start();
    	thread1.start();
    	thread2.start();
    	try {
			thread.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	try {
			thread1.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	try {
			thread2.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
    	count = myThread.getCount() + myThread1.getCount() + myThread2.getCount();
    	//count  = myThread1.getCount();
		Date end = new Date();
		Long e = System.nanoTime();
		
		double bandwidth = 0;
		double time = (e - s) / 1000000000.0;
		if (count != 0) {
			bandwidth = count / time;
		}
		System.out.println(sdf.format(end));
		// System.out.println(e - s);
		System.out.println("带宽 = " + bandwidth + " B/s");
	}
}

class WorkThread implements Runnable{
	private RateLimiter ratelimit;
	private long count;
	
	public RateLimiter getRatelimit() {
		return ratelimit;
	}

	public void setRatelimit(RateLimiter ratelimit) {
		this.ratelimit = ratelimit;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public void run(){
		// System.out.println("获取令牌，消耗 = " + acquire);
		// System.out.println("执行第" + i + "次操作");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		byte[] buff = new byte[10240];// 一次传10K
				try {
					String path = "E:\\项目\\浙江日报\\浙江日报\\CLI命令行用户手册v3.7.1.12.docx";
					InputStream in = new FileInputStream(new File(path));
					int len = 0;
					while (true) {
						if (getRatelimit().tryAcquire(20, 1, TimeUnit.SECONDS)) {
							if ((len = in.read(buff)) == -1) {						
								break;
							}
							System.out.println(Thread.currentThread().getName() + Thread.currentThread().getId() + " " + sdf.format(new Date()));
							System.out.println("获取令牌，消耗 = 20");
							count += len;
						}
						// if ((len = in.read(buff)) == -1) {
						// break;
						// }
						// count += len;
						// // double acquire = limiter.tryAcquire();
						// limiter.tryAcquire(1, 2, TimeUnit.SECONDS);
						// System.out.println("获取令牌，消耗 = ");
					}
					in.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
}
