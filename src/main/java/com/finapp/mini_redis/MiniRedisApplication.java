package com.finapp.mini_redis;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MiniRedisApplication {

	public static void main(String[] args) {
		SpringApplication.run(MiniRedisApplication.class, args);
		MiniRedisServer server = new MiniRedisServer();

        server.start();
	}

}
