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

import com.microservice.contract.PubSubTopicInterface;
import com.microservice.daemon.InMemWSTopicDirectorDaemon;
import com.microservice.util.JsonUtil;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)	//create new topic whenever autowired
public class InMemoryWebSocketTopic implements PubSubTopicInterface{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public static final int PERSISTENT_MESSAGING_CD_YES = 1;
	public static final int PERSISTENT_MESSAGING_CD_NO = 0;
	
	@Autowired
	InMemWSTopicDirectorDaemon topicDaemon;   
	
    /**
     * Max 1 additional thread will flush messages to appropriate subscribers.
     */
    private final AtomicBoolean isTopicNotProcessing = new AtomicBoolean(true);
    
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
	private final Set<String> subscriberSocketSet = new ConcurrentHashMap<String, Boolean>().keySet(Boolean.TRUE);
	
	private String name;
	private String topicId;
	
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
	public void subscribeToTopic(String subscriberSocketId) {
		synchronized(subscriberSocketSet) {
			if(subscriberSocketSet.contains(subscriberSocketId)) {
				//probably a bug and should be brought to attention
				logger.error("subscriberSocketId is already subscribed topicId:"+this.topicId+", subscriberId:"+subscriberSocketId);
				throw new RuntimeException("subscriberSocketId is already subscribed topicId:"+this.topicId+", subscriberId:"+subscriberSocketId);
			}
			subscriberSocketSet.add(subscriberSocketId);
		}
	}
	
	@Override
	public void unsubscribeFromTopic(String subscriberSocketId) {
		synchronized(subscriberSocketSet) {
			subscriberSocketSet.remove(subscriberSocketId);
		}
	}
	

	public String getTopicId() {
		return topicId;
	}
	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Set<String> getSubscriberSocketSet() {
		return subscriberSocketSet;
	}
	public ArrayDeque<WebSocketMessage> getTopicQueue() {
		return topicQueue;
	}
	public AtomicBoolean getIsTopicNotProcessing() {
		return isTopicNotProcessing;
	}
}
