package com.microservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.stereotype.Service;

import com.microservice.pubsub.InMemoryWebSocketPubSubBroker;
import com.microservice.pubsub.WebSocketMessage;
import com.microservice.spring.RedisConfig;
import com.microservice.util.JsonUtil;
import com.microservice.websocket.WebSocketMessageHandler;

/**
 * Using Redis to subscribe to topics for enabling communication between multiple instances of this microservice.
 */
@Service
public class RedisPubSubSubscriberService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public static final String REDIS_SUBSCRIBER_HANDLER_NAME = "onMessage";
	
	@Autowired
	InMemoryWebSocketPubSubBroker internalPubSubBroker;
	@Autowired
	ApplicationContext appContext;
	
	public void onMessage(String message, String channel) {
		try {
			WebSocketMessage msg = JsonUtil.fromJson(message, WebSocketMessage.class);
			long topicId = msg.getPublishTopicId();
			
			//Unsubscribe from redis => IF NO topic found in this microservice instance
			Object topicLock = WebSocketMessageHandler.topicLockMap.computeIfAbsent(topicId, k -> new Object());
			synchronized(topicLock) {
				if(!WebSocketMessageHandler.topicMap.containsKey(topicId)) {
					//unsubscribe from this topic in redis
					Object redisTopicLock = RedisConfig.redisPubSubTopicLockMap.computeIfAbsent(topicId, k ->new Object());
					//avoid duplicate subscriptions
					synchronized(redisTopicLock) {
						//manual injections, otherwise it causes a cyclic dependency.
						MessageListenerAdapter redisPubSubListener = appContext.getBean(MessageListenerAdapter.class);
						RedisMessageListenerContainer redisContainer = appContext.getBean(RedisMessageListenerContainer.class);
						
						redisContainer.removeMessageListener(redisPubSubListener, RedisConfig.redisPubSubTopicMap.get(topicId));
						RedisConfig.redisPubSubTopicMap.remove(topicId);
					}
					return;
					
				}
			}
			
			//if no issues, publish to users connected on this microservice instance.
			internalPubSubBroker.publish(msg);
		} catch(Exception e) {
			logger.error("Got error while processing message: "+message+" on channel: "+channel+ "with "+System.identityHashCode(this), e);
		}
	}
	
}
