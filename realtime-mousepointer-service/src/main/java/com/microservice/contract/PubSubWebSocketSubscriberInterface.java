package com.microservice.contract;

import java.util.List;

import com.microservice.pubsub.WebSocketMessage;

public interface PubSubWebSocketSubscriberInterface {
	
	/**
	 * Enables Push-based delivery for pub-sub system. 
	 * Message Director daemon pushes messages to subscriber for each topic it has subscribed to.
	 * @param message
	 */
	void enqueueNewMessages(List<WebSocketMessage> message);
}
