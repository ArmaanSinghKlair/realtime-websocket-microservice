package com.microservice.spring;

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

import com.microservice.service.InterServerRedisSubscriberService;

@Configuration
public class RedisConfig {
	public static final String REDIS_DEFAULT_TOPIC_NAME = "realtimeServiceTopic";
	
	@Autowired
	private Environment env;
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
//	
//	/**
//	 * Redis Message Listener specific config
//	 * @return
//	 */
//	@Bean
//	MessageListenerAdapter messageListener() { 
//	    return new MessageListenerAdapter(new InterServerMsgSubscriberService(), "onMessage");
//	}
	
	@Bean
	ChannelTopic topic() {
	    return new ChannelTopic(REDIS_DEFAULT_TOPIC_NAME);
	}
	
	@Bean
	RedisMessageListenerContainer redisContainer() {
	    RedisMessageListenerContainer container 
	      = new RedisMessageListenerContainer(); 
	    container.setConnectionFactory(redisConnectionFactory()); 
//	    container.addMessageListener(messageListener(), topic()); 
	    return container; 
	}
	
	
}
