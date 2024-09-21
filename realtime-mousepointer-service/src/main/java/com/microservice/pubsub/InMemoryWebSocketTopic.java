package com.microservice.pubsub;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.microservice.contract.PubSubWebSocketTopicInterface;
import com.microservice.util.CacheEntry;
import com.microservice.util.JsonUtil;
import com.microservice.websocket.WebSocketMessageHandler;

public class InMemoryWebSocketTopic implements PubSubWebSocketTopicInterface{
	private static final Logger logger = LoggerFactory.getLogger(InMemoryWebSocketTopic.class);

//	//register this cache to be cleaned up hourly
//	static CacheCleanupConfigBuilder<Long,Boolean> ttlCacheConfigBuilder = new CacheCleanupConfigBuilder<Long,Boolean>();
//	static {
//		ttlCacheConfigBuilder.setExpireMins(5); //entries older than 5 mins should be cleaned up
//		ttlCacheConfigBuilder.setExpireStrategy(new TTLCacheExpireStrategy<Long, Boolean>());
//	}
	public static final int TOPIC_QUEUE_PROCESSING_CD_IDLE = 0;
	public static final int TOPIC_QUEUE_PROCESSING_CD_PENDING = 1;
	public static final int TOPIC_QUEUE_PROCESSING_CD_IN_PROGRESS = 2;
	
	/**
	 * Access critical areas related to topicQueue with thread-safety
	 * 
	 */
    public final Map<Long, CacheEntry<Boolean>> subscriberIdSetLock = new ConcurrentHashMap<>();
    
    /**
     * Is topic queued to be processed in PubSubMsgDirectorDaemon.
     * If yes, don't create redundant processing entries in that daemon.
     */
    public AtomicBoolean notProcessingLock = new AtomicBoolean(true);
//    static {
//    	//register this cache 
//    	ttlCacheConfigBuilder.setCache(subscriberIdSetLock);
//    	HourlyProcessor.hourlyCacheCleanupList.add(ttlCacheConfigBuilder.build());
//    }
    

	/**
	 * Outstanding messages for this topic.
	 * Messages removed after consumption.
	 * 
	 * PUSH BASED DELIVERY: Message directory responsible for consuming each message, figuring out connecting consumers
	 * and sending the message to each consumer queue.
	 * 
	 * ConcurrentLinkedQueue ensures thread-safety for pub/sub threads VIA non-blocking behavior to avoid thread-contention.
	 * HIGH PRIORITY = least latency, LOW PRIORITY = latency NOT a priority
	 */
	private ArrayDeque<InMemoryWebSocketMessage> topicQueue = new ArrayDeque<>();
	
	/**
	 * Synchronized set using ConcurrentHashMap.keySet
	 */
	private Set<Long> subscriberSet = new ConcurrentHashMap<Long, Boolean>().keySet(Boolean.TRUE);
	
	private String name;
	private Long topicId;
	
	@Override
	public void publish(InMemoryWebSocketMessage msg) {
		try {
			if(!subscriberSet.contains(msg.getCreateSubscriberId())) {
				throw new RuntimeException("Subscriber tried to publish message in a topic which it hasn't subsribed to.");
			}
			if(msg.getPriorityCd().equals(InMemoryWebSocketMessage.PRIORITY_CD_HIGH)) {
				//TODO
				//probably offer deduplication/durability
				//TODO write to redis streams/pub-sub depending upon priority
			}
			synchronized(topicQueue) {
				topicQueue.offer(msg);
			}
			notifyMsgDirector();	//send message 
			
		} catch(Exception e) {
			logger.error("Error while publish to topic:"+this.topicId+", msg = "+JsonUtil.toJson(msg), e);
		}
	}
	
	/**
	 * Separate thread to publish to PubSubMsgDirectorDaemon. Don't want slowdown in caller.
	 * PubSubMsgDirectorDaemon handles messages that are NOT URGENT.
	 * Following code ensures ONLY 1 instance of topicId is present in queue of msg director. 
	 */
	@Async
	public void notifyMsgDirector() {
		//start broadcasting messages if not already processing
		if(notProcessingLock.compareAndExchangeRelease(true, false)) {
			flushTopicQueue();
		}
	}
	@Override
	public void subscribeToTopic(Long subscriberId) {
		synchronized(subscriberIdSetLock.computeIfAbsent(subscriberId, k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))) {
			if(subscriberSet.contains(subscriberId)) {
				//probably a bug and should be brought to attention
				logger.error("subscriberId is already subscribed topicId:"+this.topicId+", subscriberId:"+subscriberId);
				throw new RuntimeException("subscriberId is already subscribed topicId:"+this.topicId+", subscriberId:"+subscriberId);
			}
			subscriberSet.add(subscriberId);
		}
	}
	
	@Override
	public void unsubscribeFromTopic(Long subscriberId) {
		synchronized(subscriberIdSetLock.computeIfAbsent(subscriberId, k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))) {
			subscriberSet.remove(subscriberId);
		}
	}
	
	//Max length of items that can be enqueued to subscriber at ONCE.
	//If too low OR too high, it can cause latency issues (too low = consistently slow latency if many msgs, too high= slow if many messages in topicQueue)
	private static final int SUBSCRIBER_FLUSH_BUFFER_LENGTH = 100;
	@Async
	public void flushTopicQueue() {
		try {
			while(true) {
				List<InMemoryWebSocketMessage> messageBuffer = new ArrayList<>();
				while(messageBuffer.size() < SUBSCRIBER_FLUSH_BUFFER_LENGTH) {
					InMemoryWebSocketMessage curMsg = null;
					synchronized(topicQueue) {
						curMsg = topicQueue.poll();
					}
					if(curMsg == null) {
						break;	//no more items to add
					} else {
						messageBuffer.add(curMsg);
					}
				}
				if(messageBuffer.isEmpty()) {
					break;	//no more messages left to send
				}
				
				//Add buffer subscriber queue
				for(Long subscriberId: subscriberSet) {
					ArrayDeque<InMemoryWebSocketSubscriber> subscriberSockets = WebSocketMessageHandler.connectionBySubscriberMap.get(subscriberId);
					if(subscriberSockets != null) {
						for(InMemoryWebSocketSubscriber subscriberSocket: subscriberSockets) {
							subscriberSocket.enqueueNewMessages(messageBuffer);
						}
					}
				}
			}
		} catch(Exception e) {
			logger.error("Error in daemon: PubSubMsgDirectorDaemon",e);
		} finally {
			notProcessingLock.set(true);
			
			//Queue up processing if queue still has elements
			//topicQueue is critical
			synchronized(topicQueue) {
				boolean isTopicQueueEmpty = topicQueue.isEmpty();
				if(!isTopicQueueEmpty) {
					notifyMsgDirector(); 
				}
			}
		}
	}

	public Long getTopicId() {
		return topicId;
	}
	public void setTopicId(Long topicId) {
		this.topicId = topicId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Set<Long> getSubscriberSet() {
		return subscriberSet;
	}
	public ArrayDeque<InMemoryWebSocketMessage> getTopicQueue() {
		return topicQueue;
	}
}
