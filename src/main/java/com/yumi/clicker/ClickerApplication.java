package com.yumi.clicker;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@MapperScan("com.yumi.clicker.mapper")
@EnableCaching
public class ClickerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ClickerApplication.class, args);
	}

}
