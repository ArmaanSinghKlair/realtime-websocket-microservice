package com.microservice.pubsub;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.List;

import org.springframework.web.socket.WebSocketSession;

import com.microservice.contract.PubSubWebSocketSubscriberInterface;

public class InMemoryWebSocketSubscriber implements PubSubWebSocketSubscriberInterface{
	private WebSocketSession websocketSession;
	private Long subscriberId;
	
	private LocalDateTime lastCommunicationTime;
	
	public LocalDateTime lastTestTime;
	/**
	 * Push-based delivery. Subscriber can handle consumption of messages at its own pace. Decouples from producer
	 */
	private ArrayDeque<InMemoryWebSocketMessage> topicQueue = new ArrayDeque<>();

	@Override
	public void enqueueNewMessages(List<InMemoryWebSocketMessage> newMessageList) {
		synchronized(topicQueue) {
			for(InMemoryWebSocketMessage newMsg : newMessageList) {
				topicQueue.offer(newMsg);
			}
		}
		lastTestTime = LocalDateTime.now();
	}

	public WebSocketSession getWebsocketSession() {
		return websocketSession;
	}

	public void setWebsocketSession(WebSocketSession websocketSession) {
		this.websocketSession = websocketSession;
	}

	public Long getSubscriberId() {
		return subscriberId;
	}

	public void setSubscriberId(Long subscriberId) {
		this.subscriberId = subscriberId;
	}

	public LocalDateTime getLastCommunicationTime() {
		return lastCommunicationTime;
	}

	public void setLastCommunicationTime(LocalDateTime lastCommunicationTime) {
		this.lastCommunicationTime = lastCommunicationTime;
	}

	public ArrayDeque<InMemoryWebSocketMessage> getTopicQueue() {
		return topicQueue;
	}
}
