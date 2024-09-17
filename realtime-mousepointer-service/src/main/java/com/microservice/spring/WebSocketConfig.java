package com.microservice.spring;

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
	public static final long MAX_WEBSOCKET_SESSION_TIMOUT = 60*1000;	//60 seconds
	public static final long WEBSOCKET_HEARBEAT_PERIOD_SEC = 25;	//every 25 seconds
	
	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry.addHandler(new WebSocketMessageHandler(), "/websocket");
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