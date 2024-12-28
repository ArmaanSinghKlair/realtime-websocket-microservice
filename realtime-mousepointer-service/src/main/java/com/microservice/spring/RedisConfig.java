package com.microservice.spring;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import com.microservice.service.RedisPubSubSubscriberService;

@Configuration
public class RedisConfig {	
	/**
	 * Keeps track of (current microservice -> list of topicId subscriptions) mappings
	 * We're using REDIS for inter-server pub-sub mechanism
	 */
	public static ConcurrentHashMap<Long, ChannelTopic> redisPubSubTopicMap = new ConcurrentHashMap<>();
	/**
	 * Helps high-througput Thread-safety. WebSocketMessageHandler.redisTopicSubscriptionMap is used extensively. 
	 * We don't want to lock entire map, only ensure operations on the topicId are atomic
	 */
	public static final ConcurrentHashMap<Long, Object> redisPubSubTopicLockMap = new ConcurrentHashMap<>();
	
	@Autowired
	private Environment env;
	@Autowired
	private RedisPubSubSubscriberService redisPubSubSubscriberService;
	/**
	 * Configure Lettuce connection factory for connecting to redis.
	 * @return
	 */
	@Bean
	public LettuceConnectionFactory redisConnectionFactory() {
		String redisHostname = env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_HOST);
		int redisPort = Integer.parseInt(env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_PORT));
		String redisUsername = env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_USERNAME);
		String redisPassword = env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_PASSWORD);
		
		RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration(redisHostname, redisPort);
		redisConfig.setPassword(redisPassword);
		redisConfig.setUsername(redisUsername);
	    return new LettuceConnectionFactory(redisConfig);
	}
	
	@Bean
	public RedisTemplate<String, Object> redisTemplate() {
	    RedisTemplate<String, Object> template = new RedisTemplate<>();
	    template.setConnectionFactory(redisConnectionFactory());
	    return template;
	}
	
	@Bean
	RedisMessageListenerContainer redisContainer() {
	    RedisMessageListenerContainer container 
	      = new RedisMessageListenerContainer(); 
	    container.setConnectionFactory(redisConnectionFactory()); 
//	    container.addMessageListener(messageListener(), topic()); 
	    return container; 
	}
	
	/**
	 * Single listener, because each topic requires similar code handling logic
	 * @return
	 */
	@Bean
	MessageListenerAdapter redisPubSubListener() {
		return new MessageListenerAdapter(redisPubSubSubscriberService, RedisPubSubSubscriberService.REDIS_SUBSCRIBER_HANDLER_NAME);
	}
}
