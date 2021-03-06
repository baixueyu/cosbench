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

public class QosTest {
	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// RateLimiter limiter = RateLimiter.create(20.0);// 每秒放置v个令脾
		RedisUtil redis = new RedisUtil("10.180.210.55", 6379, "1q2w3e4r!");
		RateLimiterFactory rateLimiterFactory = new RateLimiterFactory();
		RateLimiter limiter = rateLimiterFactory.build("ratelimiter:im:mg2",
				1000., 30, redis);
		RateLimiter li = rateLimiterFactory.get("ratelimiter:im:mg2");
		byte[] buff = new byte[10240];// 一次传10K
		int count = 0;
		Date start = new Date();
		Long s = System.nanoTime();
		System.out.println(sdf.format(start));

		// System.out.println("获取令牌，消耗 = " + acquire);
		// System.out.println("执行第" + i + "次操作");
		try {
			String path = "E:\\项目\\浙江日报\\浙江日报\\CLI命令行用户手册v3.7.1.12.docx";
			InputStream in = new FileInputStream(new File(path));
			int len = 0;
			while (true) {
				
				if (limiter.tryAcquire(4000, 1, TimeUnit.SECONDS)) {
					if ((len = in.read(buff)) == -1) {						
						break;
					}
					System.out.println(sdf.format(new Date()));
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
