package com.microservice.contract;

import com.microservice.pubsub.InMemoryWebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketTopic;

public interface PubSubWebSocketInterface {
	/**
	 * Create topic with given name and ID;
	 * eg. (123, "Classroom"), (234, "Classroom") etc
	 * Topic ID provided by system are assumed to unique.
	 * @param topic
	 */
	void createTopic(InMemoryWebSocketTopic topic);

	/**
	 * TODO: Strict-Ordering please!!! Cannot just rely on order of messages received, due to network congestion, it could be different
	 * Write to topic
	 * @param msg
	 * @param topicId
	 */
	void writeMsgToTopic(InMemoryWebSocketMessage msg, Long topicId);
		
	void subscribeToTopic(Long topicId, Long subscriberId);
	
	void unsubscribeFromTopic(Long topicId, Long subscriberId);
	
	void deleteTopic(Long topicId);
	
	//TODO: Maybe add create topic by just NAMES
}
