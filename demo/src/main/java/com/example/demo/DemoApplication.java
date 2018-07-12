package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.demo.scala.*;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
		Analytics$ myvar = Analytics$.MODULE$;
		int minute = 8;
		System.out.println("======== ANALTICS RESULTS ========");
		String mostWatchedVideo = myvar.getMaxVideoForMinute(minute);
		System.out.printf("Video watched most in minute %d: %s ", minute, mostWatchedVideo);
		System.out.println();
		System.out.printf("Provider for video %s: %s ", mostWatchedVideo, myvar.getProviderForVideo(mostWatchedVideo).get());
		System.out.println();
		System.out.printf("Provider watched most in minute %d: %s ", minute, myvar.getMaxProviderForMinute(minute));
		System.out.println("==================================");
	}
}
