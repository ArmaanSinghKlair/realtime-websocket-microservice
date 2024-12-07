package com.microservice.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Using Redis to publish message to topics for enabling communication between multiple instances of this microservice.
 */
@Service
public class InterServerRedisPublisherService {
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	/**
	 * Publishes the message to given topic in redis
	 * @param channel
	 * @param message
	 */
	public void publish(String channel, String message) {
		redisTemplate.convertAndSend(channel, message);
	}
}
