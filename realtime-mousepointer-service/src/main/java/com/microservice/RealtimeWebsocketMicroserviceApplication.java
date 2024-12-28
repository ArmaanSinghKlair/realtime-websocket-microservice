package com.microservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class RealtimeWebsocketMicroserviceApplication {
//	private final Logger logger = LoggerFactory.getLogger(this.getClass());
		
	public static void main(String[] args) {
		ApplicationContext appContext = SpringApplication.run(RealtimeWebsocketMicroserviceApplication.class, args);
//		appContext.getBean(InMemoryWebSocketTopicDaemon.class);
	}	
}
