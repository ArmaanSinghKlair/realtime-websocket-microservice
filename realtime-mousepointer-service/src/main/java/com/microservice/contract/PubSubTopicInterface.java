package com.microservice.contract;

import com.microservice.pubsub.WebSocketMessage;

public interface PubSubTopicInterface {
	/**
	 * TODO: Strict-Ordering please!!! Cannot just rely on order of messages received, due to network congestion, it could be different
	 * Write to topic
	 * @param msg
	 * @param topicId
	 */
	void publish(WebSocketMessage msg);
		
	/**
	 * Adds subscriberSocketId to list of people who will receive all messages from this topic
	 * @param subscriberSocketId
	 */
	void subscribeToTopic(String subscriberSocketId);
	
	/**
	 * Removes subscriber from receiving any messages from Topic
	 * @param subscriberSocketId
	 */
	void unsubscribeFromTopic(String subscriberSocketId);
	
	
	//TODO: Maybe add create topic by just NAMES
}
