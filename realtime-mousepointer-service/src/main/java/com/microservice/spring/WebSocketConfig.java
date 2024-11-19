package com.microservice.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

import com.microservice.websocket.WebSocketMessageHandler;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
	public static final int MAX_WEBSOCKET_MSG_LEN = 60*1024; 	//64 kb
	public static final long MAX_WEBSOCKET_SESSION_TIMOUT = 60000*1000;	//60000 seconds
	public static final long WEBSOCKET_HEARBEAT_PERIOD_SEC = 25;	//every 25 seconds
	
	@Autowired
	ApplicationContext appContext;
	
	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry.addHandler(appContext.getBean(WebSocketMessageHandler.class), "/websocket");
//		..addInterceptors(new HttpSessionHandshakeInterceptor());...JWT
		//TODO: Add SockJS
	}
	
	/**
	 * Configure underlying tomcat params
	 * @return
	 */
	@Bean
	ServletServerContainerFactoryBean createWebSocketContainer() {
	    ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
	    container.setMaxTextMessageBufferSize(MAX_WEBSOCKET_MSG_LEN);
	    container.setMaxSessionIdleTimeout(MAX_WEBSOCKET_SESSION_TIMOUT);
	    return container;
	}
}