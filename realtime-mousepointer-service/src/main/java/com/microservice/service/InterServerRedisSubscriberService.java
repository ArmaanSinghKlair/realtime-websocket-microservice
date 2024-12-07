package com.microservice.service;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Using Redis to subscribe to topics for enabling communication between multiple instances of this microservice.
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)	//create new topic whenever autowired
public class InterServerRedisSubscriberService {
	public static final String REDIS_SUBSRIBER_HANDLER_NAME = "onMessage";
	
	public void onMessage(String message, String channel) {
		System.out.println("Got Message: "+message+" on channel: "+channel+ "with "+System.identityHashCode(this));		
	}
	
}
