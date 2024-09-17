package com.microservice.pubsub.contract;

import com.microservice.pubsub.InMemoryWebsocketMessage;

public interface PubSubWebSocketInterface {
	/**
	 * Create topic with given name and ID;
	 * eg. (123, "Classroom"), (234, "Classroom") etc
	 * Topic ID provided by system are assumed to unique.
	 * @param topicId
	 * @param topicName
	 */
	void createTopic(Long topicId, String topicName);

	/**
	 * TODO: Strict-Ordering please!!! Cannot just rely on order of messages received, due to network congestion, it could be different
	 * Write to topic
	 * @param msg
	 * @param topicId
	 */
	void writeMsgToTopic(InMemoryWebsocketMessage msg, Long topicId);
		
	void subscribeToTopic(Long topicId, Long subscriberId);
	
	void unsubscribeFromTopic(Long topicId, Long subscriberId);
	
	void deleteTopic(Long topicId);
	
	//TODO: Maybe add create topic by just NAMES
}
