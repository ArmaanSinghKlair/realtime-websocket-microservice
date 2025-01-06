package com.microservice.contract;

import org.springframework.context.ApplicationContext;

import com.microservice.pubsub.WebSocketMessage;

public interface PubSubBrokerInterface {
	
	/**
	 * Write to topic
	 * @param msg
	 * @param topicId
	 */
	void publish(WebSocketMessage msg);
	
	/**
	 * Adds subscriber to list of listeners for topic.
	 * @param topicId
	 * @param subscriberSocketId
	 */
	void subscribeToTopic(String topicId, String subscriberSocketId, ApplicationContext appContext);
	
	/**
	 * Removes subscriber from the list of listeners from topic
	 * @param topicId
	 * @param subscriberSocketId
	 */
	void unsubscribeFromTopic(String topicId, String subscriberSocketId);
	
	/**
	 * Delete topic
	 * @param topicId
	 */
	void deleteTopic(String topicId);
}
