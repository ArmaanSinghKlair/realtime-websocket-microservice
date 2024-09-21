package com.microservice.contract;

import com.microservice.pubsub.InMemoryWebSocketMessage;

public interface PubSubWebSocketTopicInterface {
	/**
	 * TODO: Strict-Ordering please!!! Cannot just rely on order of messages received, due to network congestion, it could be different
	 * Write to topic
	 * @param msg
	 * @param topicId
	 */
	void publish(InMemoryWebSocketMessage msg);
		
	/**
	 * Adds subscriberId to list of people who will receive all messages from this topic
	 * @param subscriberId
	 */
	void subscribeToTopic(Long subscriberId);
	
	/**
	 * Removes subscriber from receiving any messages from Topic
	 * @param subscriberId
	 */
	void unsubscribeFromTopic(Long subscriberId);
	
	
	//TODO: Maybe add create topic by just NAMES
}
