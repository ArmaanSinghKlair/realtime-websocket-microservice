package com.microservice.pubsub;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.microservice.contract.PubSubBrokerInterface;
import com.microservice.websocket.WebSocketMessageHandler;

/**
 * In-memory pub-sub message broker tailored for websockets
 * I have a broker class, where publishes calls publish method, which calls the publish method of the assoc topic class which adds to queue in topic class itself and returns. It also starts up a thread if not already started that looks over the topic queue and sort of flushes it async therfore decoupling the publisher from the broker. I have maintained a 1 thread/1 topic standard that is triggered by any incoming message to the queue. Now this thread thread-safely polls the queue messages one by one and adds them to a in-memory buffer with a max limit of 100 and adds the 100 messages to the subscriber queue and returns (ie enqueueNewMessages method call in subscriber class). Now this subscriber class also follows the 1 thread/1 subscriber where this thread is triggered by incoming messages and sort of flushes them async therefore decoupleing the subscriber from the broker
 */
@Component
public class InMemoryWebSocketPubSubBroker implements PubSubBrokerInterface{
//	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * TODO: Strict-Ordering please!!! Cannot just rely on order of messages received, due to network congestion, it could be different
	 * Write to topic
	 * Also does a cleanup WebSocketPubSubBroker.topicMap if no-one subscribed to intended topic
	 * @param msg
	 * @param topicId
	 */
	public void publish(WebSocketMessage msg) {
		String publishTopicId = msg.getTargetTopicId();
		
		Object topicLock = WebSocketMessageHandler.topicLockMap.computeIfAbsent(publishTopicId, k -> new Object());
		synchronized(topicLock) {
			//shouldn't really happen
			if(!WebSocketMessageHandler.topicMap.containsKey(publishTopicId)){
				throw new RuntimeException("Topic ID ("+publishTopicId+") does not exist. WebSocketPubSubBroker.publish");
			}
			
			//CLEANUP topicMap if NO subscribers found for topic
			InMemoryWebSocketTopic topic = WebSocketMessageHandler.topicMap.get(publishTopicId);
			synchronized(topic.getSubscriberSocketSet()) {
				if(topic.getSubscriberSocketSet().isEmpty()) {
					this.deleteTopic(publishTopicId);
					return;
				}
			}
		
			topic.publish(msg);
		}
	}

	
	/**
	 * Adds subscriber to list of listeners for topic.
	 * Creates a new topic, if not already present.
	 * @param topicId
	 * @param subscriberSocketId
	 */
	public void subscribeToTopic(String topicId, String subscriberSocketId, ApplicationContext appContext) {
		Object topicLock = WebSocketMessageHandler.topicLockMap.computeIfAbsent(topicId, k -> new Object());
		synchronized(topicLock) {
			//Create topic if not present
			if(!WebSocketMessageHandler.topicMap.containsKey(topicId)){
				InMemoryWebSocketTopic topic = appContext.getBean(InMemoryWebSocketTopic.class);
				topic.setName("TopicId: "+topicId);
				topic.setTopicId(topicId);
				WebSocketMessageHandler.topicMap.put(topic.getTopicId(), topic);
			}
			WebSocketMessageHandler.topicMap.get(topicId).subscribeToTopic(subscriberSocketId);
		}
	}
	
	
	/**
	 * Removes subscriber from the list of listeners from topic
	 * @param topicId
	 * @param subscriberSocketId
	 */
	public void unsubscribeFromTopic(String topicId, String subscriberSocketId) {
		Object topicLock = WebSocketMessageHandler.topicLockMap.computeIfAbsent(topicId, k -> new Object());
		synchronized(topicLock) {
			if(!WebSocketMessageHandler.topicMap.containsKey(topicId)){
				throw new RuntimeException("Topic ID ("+topicId+") does not exist.");
			}
			WebSocketMessageHandler.topicMap.get(topicId).unsubscribeFromTopic(subscriberSocketId);
		}
	}

	/**
	 * Delete topic
	 * @param topicId
	 */
	public void deleteTopic(String topicId) {
		Object topicLock = WebSocketMessageHandler.topicLockMap.computeIfAbsent(topicId, k -> new Object());
		synchronized(topicLock) {
			WebSocketMessageHandler.topicMap.remove(topicId);			
		}
	}
}
