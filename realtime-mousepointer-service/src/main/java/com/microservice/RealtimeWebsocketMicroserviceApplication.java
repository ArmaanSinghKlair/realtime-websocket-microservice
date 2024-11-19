package com.microservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class RealtimeWebsocketMicroserviceApplication {
	private static final Logger logger = LoggerFactory.getLogger(RealtimeWebsocketMicroserviceApplication.class);
		
	public static void main(String[] args) {
		ApplicationContext appContext = SpringApplication.run(RealtimeWebsocketMicroserviceApplication.class, args);
//		appContext.getBean(InMemoryWebSocketTopicDaemon.class);
	}	
}
