package com.microservice.pubsub;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.microservice.contract.PubSubWebSocketTopicInterface;
import com.microservice.daemon.InMemWSTopicDirectorDaemon;
import com.microservice.util.JsonUtil;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)	//create new topic whenever autowired
public class InMemoryWebSocketTopic implements PubSubWebSocketTopicInterface{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

//	//register this cache to be cleaned up hourly
//	static CacheCleanupConfigBuilder<Long,Boolean> ttlCacheConfigBuilder = new CacheCleanupConfigBuilder<Long,Boolean>();
//	static {
//		ttlCacheConfigBuilder.setExpireMins(5); //entries older than 5 mins should be cleaned up
//		ttlCacheConfigBuilder.setExpireStrategy(new TTLCacheExpireStrategy<Long, Boolean>());
//	}
	public static final int TOPIC_QUEUE_PROCESSING_CD_IDLE = 0;
	public static final int TOPIC_QUEUE_PROCESSING_CD_PENDING = 1;
	public static final int TOPIC_QUEUE_PROCESSING_CD_IN_PROGRESS = 2;
	
	@Autowired
	InMemWSTopicDirectorDaemon topicDaemon;    
    /**
     * Is any daemon currently NOT processing topicQueue
     */
    private final AtomicBoolean isTopicNotProcessing = new AtomicBoolean(true);
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
	private final ArrayDeque<WebSocketMessage> topicQueue = new ArrayDeque<>();
	
	/**
	 * Synchronized set using ConcurrentHashMap.keySet
	 */
	private final Set<Long> subscriberSet = new ConcurrentHashMap<Long, Boolean>().keySet(Boolean.TRUE);
	
	private String name;
	private Long topicId;
	
	@Override
	public void publish(WebSocketMessage msg) {
		try {
			synchronized(topicQueue) {
				topicQueue.offer(msg);
			}
			//send msg to subscribers on current microserice
			notifyMsgDirector();	 			
		} catch(Exception e) {
			logger.error("Error while publish to topic:"+this.topicId+", msg = "+JsonUtil.toJson(msg), e);
		}
	}
	
	/**
	 * PubSubMsgDirectorDaemon handles messages that are NOT URGENT.
	 * Following code ensures ONLY 1 instance of topicId is present in queue of msg director. 
	 */
	public void notifyMsgDirector() {
		//start broadcasting messages if not already processing
		if(isTopicNotProcessing.compareAndExchangeRelease(true, false)) {
			try {
				topicDaemon.flushTopicQueue(this);
			} catch(Exception e) {
				//task rejection errors
				isTopicNotProcessing.set(true);	//clear sempahore
			}
		}
	}
	
	@Override
	public void subscribeToTopic(Long subscriberId) {
		synchronized(subscriberSet) {
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
		synchronized(subscriberSet) {
			subscriberSet.remove(subscriberId);
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
	public ArrayDeque<WebSocketMessage> getTopicQueue() {
		return topicQueue;
	}
	public AtomicBoolean getIsTopicNotProcessing() {
		return isTopicNotProcessing;
	}
}
