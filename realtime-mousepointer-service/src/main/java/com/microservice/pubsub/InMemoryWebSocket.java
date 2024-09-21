package com.microservice.pubsub;

import java.util.LinkedList;
import java.util.List;

import org.springframework.web.socket.WebSocketSession;

/**
 * Meta-data about single websocket connection.
 * Also contains its own queue of messages
 * 
 * @author Armaan Singh Klair
 */
public class InMemoryWebSocket {
	private Long subscriberId;
	private WebSocketSession webSocketSession;
	private List<InMemoryWebSocketMessage> subscriberConsumeQueue = new LinkedList<>();
	
	public Long getSubscriberId() {
		return subscriberId;
	}
	public void setSubscriberId(Long subscriberId) {
		this.subscriberId = subscriberId;
	}
	public WebSocketSession getWebSocketSession() {
		return webSocketSession;
	}
	public void setWebSocketSession(WebSocketSession webSocketSession) {
		this.webSocketSession = webSocketSession;
	}
	public List<InMemoryWebSocketMessage> getSubscriberConsumeQueue() {
		return subscriberConsumeQueue;
	}
	public void setSubscriberConsumeQueue(List<InMemoryWebSocketMessage> subscriberConsumeQueue) {
		this.subscriberConsumeQueue = subscriberConsumeQueue;
	}
	
	
}
