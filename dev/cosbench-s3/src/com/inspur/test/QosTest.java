package com.inspur.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.inspur.ratelimit.RateLimiter;
import com.inspur.ratelimit.RateLimiterFactory;

public class QosTest {
	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// RateLimiter limiter = RateLimiter.create(20.0);// ÿ�����v����Ƣ
		RateLimiterFactory rateLimiterFactory = new RateLimiterFactory();
		RateLimiter limiter = rateLimiterFactory.build("ratelimiter:im:msg",
				10.0, 30);
		byte[] buff = new byte[10240];// һ�δ�10K
		int count = 0;
		Date start = new Date();
		Long s = System.nanoTime();
		System.out.println(sdf.format(start));

		// System.out.println("��ȡ���ƣ����� = " + acquire);
		// System.out.println("ִ�е�" + i + "�β���");
		try {
			String path = "E:\\CLI�������û��ֲ�v3.7.1.8.docx";
			InputStream in = new FileInputStream(new File(path));
			int len = 0;
			while (true) {
				if (limiter.tryAcquire(1, 2, TimeUnit.SECONDS)) {
					if ((len = in.read(buff)) == -1) {
						System.out.println("��ȡ���ƣ����� = ");
						break;
					}
					count += len;
				}
				// if ((len = in.read(buff)) == -1) {
				// break;
				// }
				// count += len;
				// // double acquire = limiter.tryAcquire();
				// limiter.tryAcquire(1, 2, TimeUnit.SECONDS);
				// System.out.println("��ȡ���ƣ����� = ");
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
		System.out.println("���� = " + bandwidth + " B/s");
	}
}
