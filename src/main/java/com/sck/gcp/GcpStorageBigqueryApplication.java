package com.sck.gcp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages ={"com.sck"})
public class GcpStorageBigqueryApplication {

	public static void main(String[] args) {
		SpringApplication.run(GcpStorageBigqueryApplication.class, args);
	}
}
