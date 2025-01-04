package com.microservice.service;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import com.microservice.pubsub.InMemoryWebSocketPubSubBroker;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.WebSocketMessage;
import com.microservice.util.JsonUtil;
import com.microservice.websocket.WebSocketMessageHandler;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)	//create new listener whenever autowired.
public class RedisStreamListener implements StreamListener<String, ObjectRecord<String, String>> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
    @Autowired
	InMemoryWebSocketPubSubBroker internalPubSubBroker;
	@Autowired
	ApplicationContext appContext;
    @Autowired
    RedisService redisService;
	/**
	 * Previous redis stream record id recieved in this listnere
	 */
	private String prevousPersistenceId;
	private String subscriberSocketId;
	
    @Override
    public void onMessage(ObjectRecord<String, String> record) {
    	try {
    		String streamMsgId = record.getId().getValue();
			WebSocketMessage msg = JsonUtil.fromJson(record.getValue(), WebSocketMessage.class);
			msg.setPersistenceId(streamMsgId);
			msg.setPrevousPersistenceId(prevousPersistenceId);
			String topicId = msg.getPublishTopicId();
			
			//Unsubscribe from redis => IF NO topic found in this microservice instance
			Object topicLock = WebSocketMessageHandler.topicLockMap.computeIfAbsent(topicId, k -> new Object());
			synchronized(topicLock) {
				if(!WebSocketMessageHandler.topicMap.containsKey(topicId)) {
					//unsubscribe from this topic in redis
					redisService.unsubscribeFromStream(subscriberSocketId, topicId);
					return;
				}
			}
			
			//Unsubscribe, if socket closed
			InMemoryWebSocketSubscriber subscriberSocket = WebSocketMessageHandler.subscriberSocketMap.get(subscriberSocketId);
			if(subscriberSocket == null) {
				redisService.unsubscribeFromStream(subscriberSocketId, topicId);
				return;
			}
			
			//if no issues, enqueue messages in subscriber message queue.
			subscriberSocket.enqueueNewMessages(Arrays.asList(msg));
			prevousPersistenceId = streamMsgId;			
		} catch(Exception e) {
			logger.error("Got error while processing message: " + record.toString(), e);
		}
    }
	public String getPrevousPersistenceId() {
		return prevousPersistenceId;
	}
	public void setPrevousPersistenceId(String prevousPersistenceId) {
		this.prevousPersistenceId = prevousPersistenceId;
	}
	public String getSubscriberSocketId() {
		return subscriberSocketId;
	}
	public void setSubscriberSocketId(String subscriberSocketId) {
		this.subscriberSocketId = subscriberSocketId;
	}
}