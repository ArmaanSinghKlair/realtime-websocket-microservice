package com.microservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Service;

import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.spring.RedisConfig;
import com.microservice.websocket.WebSocketMessageHandler;

/**
 * Using Redis to publish message to topics for enabling communication between multiple instances of this microservice.
 */
@Service
public class RedisService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	@Autowired
	private StreamMessageListenerContainer<String, ObjectRecord<String, String>> redisStreamListenerContainer;
	@Autowired
	private ApplicationContext appContext;
	
	/**
	 * Publishes the message to given topic in Redis PubSub
	 * @param channel
	 * @param message
	 */
	public void publish(String channel, String message) {
		redisTemplate.convertAndSend(channel, message);
	}
	
	/**
	 * Publishes to Redis Stream with specific key
	 * @param key
	 * @param value
	 * @return
	 */
	public RecordId produceStreamRecord(String topicId, String value) {
		ObjectRecord<String, String> record = StreamRecords.newRecord()
				.ofObject(value)
				.withStreamKey(topicId);
		
		RecordId recordId = redisTemplate.opsForStream().add(record);
		
		if(recordId == null) {
			logger.error("Error sending stream event (Key: "+topicId+", Value: "+value+")");
			throw new RuntimeException("Error sending stream event (Key: "+topicId+", Value: "+value+")");
		}
		return recordId;
	}
	
	/**
	 * Subscribes to redis stream via key. Attaches an async listener to it.
	 * @param streamKey
	 * @param lastMessageId
	 * @return
	 */
	public void subscriberToStream(String subscriberSocketId, String topicId, String lastMessageId) {
		String streamKey = RedisConfig.getRedisStreamSubscriptionMap(subscriberSocketId, topicId);
		
		Object redisStreamLock = RedisConfig.redisStreamSubscriptionLockMap.computeIfAbsent(streamKey, k ->new Object());
		//avoid duplicate subscriptions
		synchronized(redisStreamLock) {	
			if(RedisConfig.redisStreamSubscriptionMap.contains(streamKey)) {
				return;	//probably a race condition
			}
			StreamOffset<String> offset = null;
			
			if(lastMessageId == null) {
				offset = StreamOffset.fromStart(topicId);	//from starting
			} else {
				offset = StreamOffset.create(topicId, ReadOffset.from(lastMessageId));	//start from last message Id we saw
			}
			RedisStreamListener streamListener = appContext.getBean(RedisStreamListener.class);
			streamListener.setSubscriberSocketId(subscriberSocketId);
			Subscription sub= redisStreamListenerContainer.receive(offset, streamListener);
			RedisConfig.redisStreamSubscriptionMap.put(streamKey, sub);
		}
	}
	
	/**
	 * Stop listening to stream for provided websocket.
	 * @param subscriberSocketId
	 * @param streamKey
	 */
	public void unsubscribeFromStream(String subscriberSocketId, String topicId) {
		String streamKey = RedisConfig.getRedisStreamSubscriptionMap(subscriberSocketId, topicId);
		
		Object redisStreamLock = RedisConfig.redisStreamSubscriptionLockMap.computeIfAbsent(streamKey, k ->new Object());
		//avoid duplicate subscriptions
		synchronized(redisStreamLock) {	
			Subscription prevSubscription = RedisConfig.redisStreamSubscriptionMap.remove(streamKey);
			if(prevSubscription == null) {
				return;	//probably a race condition
			}
			prevSubscription.cancel();
		}
	}
}
