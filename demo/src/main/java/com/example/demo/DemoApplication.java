package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.demo.scala.*;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
		Analytics$ myvar = Analytics$.MODULE$;
		System.out.println(myvar.getProviderForVideo("video3").get());
		System.out.println("MAX::::"+myvar.getMaxVideoForMin(7));
	}
}
