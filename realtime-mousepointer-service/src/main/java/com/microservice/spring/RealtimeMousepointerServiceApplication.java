package com.microservice.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RealtimeMousepointerServiceApplication {
	private static final Logger logger = LoggerFactory.getLogger(RealtimeMousepointerServiceApplication.class);
//	  private static final Logger logger = LogManager.getLogger(RealtimeMousepointerServiceApplication.class);
	  
	public static void main(String[] args) {
		SpringApplication.run(RealtimeMousepointerServiceApplication.class, args);
		logger.debug("DEBUG MESSAGE HERE");
		logger.warn("WARN MESSAGE HERE");
		logger.trace("TRACE MESSAGE HERE");
		logger.error("ERROR MESSAGE HERE", new RuntimeException("Yoo calm down chill bruv"));
		logger.info("INFO MESSAGE HERE");
	}

}
