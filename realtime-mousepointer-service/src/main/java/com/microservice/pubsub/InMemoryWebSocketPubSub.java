package com.microservice.pubsub;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.microservice.pubsub.contract.PubSubWebSocketInterface;

public class InMemoryWebSocketPubSub implements PubSubWebSocketInterface{
	/**
	 * Outstanding messages for each topic.
	 * Messages removed after consumption.
	 * 
	 * ConcurrenHashMap ensures thread-safety for topic removal/additions
	 * ConcurrentLinkedQueue ensures thread-safety for pub/sub threads
	 */
	Map<Long, ConcurrentLinkedQueue<InMemoryWebsocketMessage>> topicQueueMap = new ConcurrentHashMap<>();
	Map<Long, Set<Long>> topicSubscribeXrefMap = new ConcurrentHashMap<>();
	
	@Override
	public void createTopic(Long topicId, String topicName) {
		if(topicQueueMap.containsKey(topicId)){
			throw new IllegalArgumentException("Topic ID ("+topicId+") already exists for topicName ("+topicName+").");
		}
		//Insert into topicSubscriberXref before topicQueueMap to avoid race-conditions in-case messages consumed from queue before topicSubscriberXref initialized
		topicSubscribeXrefMap.put(topicId, new HashSet<>());
		topicQueueMap.put(topicId, new ConcurrentLinkedQueue<>());
		
	}

	@Override
	public void writeMsgToTopic(InMemoryWebsocketMessage msg, Long topicId) {
		if(!topicQueueMap.containsKey(topicId)){
			throw new IllegalArgumentException("Topic ID ("+topicId+") does not exist.");
		}
		topicQueueMap.get(topicId).offer(msg);
		
	}

	@Override
	public void subscribeToTopic(Long topicId, Long subscriberId) {
		if(!topicQueueMap.containsKey(topicId)){
			throw new IllegalArgumentException("Topic ID ("+topicId+") does not exist.");
		}
		if(!topicSubscribeXrefMap.containsKey(topicId)) {
			//logger.error("Topic initialization error. Topic exists in topicQueue but not in topicSubscribeXrefMap.");
			throw new RuntimeException("Topic initialization error.");
		}
		topicSubscribeXrefMap.get(topicId).add(subscriberId);
	}

	@Override
	public void unsubscribeFromTopic(Long topicId, Long subscriberId) {
		if(!topicQueueMap.containsKey(topicId)){
			throw new IllegalArgumentException("Topic ID ("+topicId+") does not exist.");
		}
		if(!topicSubscribeXrefMap.containsKey(topicId)) {
			//logger.error("Topic initialization error. Topic exists in topicQueue but not in topicSubscribeXrefMap.");
			throw new RuntimeException("Topic initialization error.");
		}
		topicSubscribeXrefMap.get(topicId).remove(subscriberId);
	}

	@Override
	public void deleteTopic(Long topicId) {
		topicQueueMap.remove(topicId);
		topicSubscribeXrefMap.remove(topicId);
	}

}
