package com.microservice.spring;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import com.microservice.service.RedisPubSubListener;


@Configuration
public class RedisConfig {	
	
	@Autowired
	private Environment env;
	@Autowired
	private RedisPubSubListener redisPubSubListenerService;
	
	public static final int REDIS_STREAM_POLL_TIMEOUT_MS = 100;
	/**
	 * Keeps track of (current microservice -> list of topicId subscriptions) mappings
	 * We're using REDIS PUB-SUB for inter-server communication mechanism
	 */
	public static ConcurrentHashMap<String, ChannelTopic> redisPubSubTopicMap = new ConcurrentHashMap<>();
	/**
	 * Helps high-througput Thread-safety. WebSocketMessageHandler.redisPubSubTopicMap is used extensively. 
	 * We don't want to lock entire map, only ensure operations on the topicId are atomic
	 */
	public static final ConcurrentHashMap<String, Object> redisPubSubTopicLockMap = new ConcurrentHashMap<>();
	
	/**
	 *  <subscriberSocketId-topicId , Redis STREAM subscription>.
	 *  Use RedisConfig.getSocketRedisStreamMapKey method for generating keys
	 */
	public static ConcurrentHashMap<String, Subscription> redisStreamSubscriptionMap = new ConcurrentHashMap<>();
	public static final ConcurrentHashMap<String, Object> redisStreamSubscriptionLockMap = new ConcurrentHashMap<>();
	
	/**
	 * Uniquely identifies a redis stream subscription.
	 * @param subscriberSocketId
	 * @param topicId
	 * @return
	 */
	public static String getRedisStreamSubscriptionMap(String subscriberSocketId, String topicId) {
		return subscriberSocketId+"#"+topicId;
	}
	
	/**
	 * Configure Lettuce connection factory for connecting to redis.
	 * @return
	 */
//	@Bean
//	public RedisConnectionFactory redisConnectionFactory() {
//		String redisHostname = env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_HOST);
//		int redisPort = Integer.parseInt(env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_PORT));
//		String redisUsername = env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_USERNAME);
//		String redisPassword = env.getProperty(PropertyConfig.PROPERTY_KEY_REDIS_PASSWORD);
//		
//		RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration(redisHostname, redisPort);
//		redisConfig.setPassword(redisPassword);
//		redisConfig.setUsername(redisUsername);
//	    return new LettuceConnectionFactory(redisConfig);
//	}
	
	@Bean
	public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
	    RedisTemplate<String, Object> template = new RedisTemplate<>();
	    template.setConnectionFactory(redisConnectionFactory);
	    return template;
	}
	
	/**
	 * Async listener container for redis pub-sub. Reuses 1 connection for all requests (low overhead, multiplexing)
	 * @return
	 */
	@Bean
	RedisMessageListenerContainer redisPubSubListenerContainer(RedisConnectionFactory redisConnectionFactory) {
	    RedisMessageListenerContainer container 
	      = new RedisMessageListenerContainer(); 
	    container.setConnectionFactory(redisConnectionFactory); 
	    return container; 
	}
	
	/**
	 * Async listener 
	 * @return
	 */
	@Bean
	public StreamMessageListenerContainer<String, ObjectRecord<String, String>> subscription(RedisConnectionFactory redisConnectionFactory) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, String>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofMillis(REDIS_STREAM_POLL_TIMEOUT_MS))
                .targetType(String.class)
                .build();
        StreamMessageListenerContainer<String, ObjectRecord<String, String>> container = StreamMessageListenerContainer.create(redisConnectionFactory, options);
        container.start();
        return container;
	}
	
	/**
	 * Single listener, because each topic requires similar code handling logic
	 * @return
	 */
	@Bean
	MessageListenerAdapter messageListenerAdapter() {
		return new MessageListenerAdapter(redisPubSubListenerService, RedisPubSubListener.REDIS_SUBSCRIBER_HANDLER_NAME);
	}
}
