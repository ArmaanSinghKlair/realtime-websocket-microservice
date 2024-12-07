package com.microservice.pubsub;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microservice.util.CacheEntry;

/**
 * In-memory pub-sub message broker tailored for websockets
 * I have a broker class, where publishes calls publish method, which calls the publish method of the assoc topic class which adds to queue in topic class itself and returns. It also starts up a thread if not already started that looks over the topic queue and sort of flushes it async therfore decoupling the publisher from the broker. I have maintained a 1 thread/1 topic standard that is triggered by any incoming message to the queue. Now this thread thread-safely polls the queue messages one by one and adds them to a in-memory buffer with a max limit of 100 and adds the 100 messages to the subscriber queue and returns (ie enqueueNewMessages method call in subscriber class). Now this subscriber class also follows the 1 thread/1 subscriber where this thread is triggered by incoming messages and sort of flushes them async therefore decoupleing the subscriber from the broker
 */
public class WebSocketPubSubBroker {
	private static final Logger logger = LoggerFactory.getLogger(WebSocketPubSubBroker.class);
	
	//register this cache to be cleaned up hourly
//	static CacheCleanupConfigBuilder<Long,Boolean> ttlCacheConfigBuilder = new CacheCleanupConfigBuilder<Long,Boolean>();
//	static {
//		ttlCacheConfigBuilder.setExpireMins(5); //entries older than 5 mins should be cleaned up
//		ttlCacheConfigBuilder.setExpireStrategy(new TTLCacheExpireStrategy<Long, Boolean>());
//	}
	/**
	 * Deduplication at pub-sub broker level
	 * Rolling cache. All entries before 5 mins can be cleared safely.
	 * Rolling cache avoids race conditions with cleanup daemon and recently created topics
	 * 
	 */
	public static final Map<Long, CacheEntry<Boolean>> topicLockMap = new ConcurrentHashMap<>();
//	static {
//		//register this cache 
//    	ttlCacheConfigBuilder.setCache(topicLockMap);
//    	HourlyProcessor.hourlyCacheCleanupList.add(ttlCacheConfigBuilder.build());
//    }
	/**
	 * Map of all topics by Id
	 * TODO: clear this when a topic closes OR when no subscribers are in this topic. This will trigger Redis topicId listening cleanup as well.
	 * <TopicId, Topic>
	 */
	public static ConcurrentHashMap<Long, InMemoryWebSocketTopic> topicMap = new ConcurrentHashMap<>();
	    
	/**
	 * Create topic with given name and ID;
	 * eg. (123, "Classroom"), (234, "Classroom") etc
	 * Topic ID provided by system are assumed to unique.
	 * @param topic
	 */
	public static void createTopic(InMemoryWebSocketTopic topic) {
		synchronized(topicLockMap.computeIfAbsent(topic.getTopicId(), k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))) {
			if(topicMap.containsKey(topic.getTopicId())){
				throw new IllegalArgumentException("Topic ID ("+topic.getTopicId()+") already exists for topicName ("+topic.getName()+").");
			}
			topicMap.put(topic.getTopicId(), topic);
		}
		
	}

	/**
	 * TODO: Strict-Ordering please!!! Cannot just rely on order of messages received, due to network congestion, it could be different
	 * Write to topic
	 * @param msg
	 * @param topicId
	 */
	public static void publish(WebSocketMessage msg) {
		synchronized(topicLockMap.computeIfAbsent(msg.getPublishTopicId(), k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))) {
			if(!topicMap.containsKey(msg.getPublishTopicId())){
				logger.error("Topic ID ("+msg.getPublishTopicId()+") does not exist. WebSocketPubSubBroker.publish");
			}
			topicMap.get(msg.getPublishTopicId()).publish(msg);
		}

//			if(!topicMap.get(msg.getTopicId()).getSubscriberSet().contains(msg.getCreateSubscriberId())) {
//				logger.error("Subscriber ("+msg.getCreateSubscriberId()+") tried to send messages to topicId ("+msg.getTopicId()+") when its not even subscribed. Probably misordering of messages OR cleanup issue of subscribers");
//				return;	//fail silently
//			}

	}

	
	/**
	 * Adds subscriber to list of listeners for topic
	 * @param topicId
	 * @param subscriberId
	 */
	public static boolean subscribeToTopic(Long topicId, Long subscriberId) {
		synchronized(topicLockMap.computeIfAbsent(topicId, k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))) {
			if(!topicMap.containsKey(topicId)){
				throw new IllegalArgumentException("Topic ID ("+topicId+") does not exist.");
			}
			topicMap.get(topicId).subscribeToTopic(subscriberId);
		}
		return true;
	}
	
	
	/**
	 * Removes subscriber from the list of listeners from topic
	 * @param topicId
	 * @param subscriberId
	 */
	public static void unsubscribeFromTopic(Long topicId, Long subscriberId) {
		synchronized(topicLockMap.computeIfAbsent(topicId, k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))) {
			if(!topicMap.containsKey(topicId)){
				throw new IllegalArgumentException("Topic ID ("+topicId+") does not exist.");
			}
			topicMap.get(topicId).unsubscribeFromTopic(subscriberId);
		}
	}

	/**
	 * Delete topic
	 * @param topicId
	 */
	public static void deleteTopic(Long topicId) {
		synchronized(topicLockMap.computeIfAbsent(topicId, k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))) {
			if(!topicMap.containsKey(topicId)){
				throw new IllegalArgumentException("Topic ID ("+topicId+") does not exist.");
			}
			topicMap.remove(topicId);			
		}
	}

}
