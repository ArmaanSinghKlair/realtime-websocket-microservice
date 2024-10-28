package com.microservice.test;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.microservice.daemon.InMemoryWebSocketTopicDaemon;
import com.microservice.pubsub.InMemoryWebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketPubSubBroker;
import com.microservice.websocket.WebSocketMessageHandler;

@Component
public class PubSubBrokerTest implements CommandLineRunner{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	@Autowired
	InMemoryWebSocketTopicDaemon topicDaemon;
	@Autowired
	InMemoryWebSocketTopic topic;
	@Autowired
	ApplicationContext appContext;
	
	@Override
	public void run(String... args) throws Exception {
//		testPubSub();
		testSingleTopic();
	}
	
	@Deprecated
	private void testSingleTopic() {
		InMemoryWebSocketTopic topic1 = appContext.getBean(InMemoryWebSocketTopic.class);
		topic1.setName("Class room 1");
		topic1.setTopicId(1l);
	}
	
	/**
	 * Sends messages 
	 */
	@Deprecated
	private void testPubSub() {
		InMemoryWebSocketSubscriber sub1 = appContext.getBean(InMemoryWebSocketSubscriber.class);
		sub1.setLastPingReceiveTime(LocalDateTime.now());
		sub1.setSubscriberId(1L);
		InMemoryWebSocketSubscriber sub2 = appContext.getBean(InMemoryWebSocketSubscriber.class);
		sub2.setLastPingReceiveTime(LocalDateTime.now());
		sub2.setSubscriberId(2L);
		InMemoryWebSocketSubscriber sub3 = appContext.getBean(InMemoryWebSocketSubscriber.class);
		sub3.setLastPingReceiveTime(LocalDateTime.now());
		sub3.setSubscriberId(3L);
		//now mimic connecting to websockets
		WebSocketMessageHandler.connectionBySubscriberMap.put(1l, new ArrayDeque<>(Arrays.asList(sub1)));
		WebSocketMessageHandler.connectionBySubscriberMap.put(2l, new ArrayDeque<>(Arrays.asList(sub2)));
		WebSocketMessageHandler.connectionBySubscriberMap.put(3l, new ArrayDeque<>(Arrays.asList(sub3)));
		
		//Creat topics
		InMemoryWebSocketTopic topic1 = appContext.getBean(InMemoryWebSocketTopic.class);
		topic1.setName("Topic 1");
		topic1.setTopicId(10l);
		
		
		InMemoryWebSocketTopic topic2 = appContext.getBean(InMemoryWebSocketTopic.class);
		topic2.setName("Topic 2");
		topic2.setTopicId(20l);
		
		
		InMemoryWebSocketTopic topic3 = appContext.getBean(InMemoryWebSocketTopic.class);
		topic3.setName("Topic 3");
		topic3.setTopicId(30l);
		
		//Register topics		
		WebSocketPubSubBroker.createTopic(topic1);
		WebSocketPubSubBroker.createTopic(topic2);
		WebSocketPubSubBroker.createTopic(topic3);
		
		//Subscriber first
		WebSocketPubSubBroker.subscribeToTopic(10l, 1l);
		WebSocketPubSubBroker.subscribeToTopic(10l, 2l);
		WebSocketPubSubBroker.subscribeToTopic(10l, 3l);
		
		WebSocketPubSubBroker.subscribeToTopic(20l, 1l);
		WebSocketPubSubBroker.subscribeToTopic(20l, 2l);
		WebSocketPubSubBroker.subscribeToTopic(20l, 3l);
		
		WebSocketPubSubBroker.subscribeToTopic(30l, 1l);
		WebSocketPubSubBroker.subscribeToTopic(30l, 2l);
		WebSocketPubSubBroker.subscribeToTopic(30l, 3l);
		
		Long startTime = System.currentTimeMillis();
		logger.debug("Starting to send msgs");
		//send messages
		for(int i=0;i<1;i++) {
			InMemoryWebSocketMessage msg1 = new InMemoryWebSocketMessage();
			msg1.setCreateSubscriberId(1l);
			msg1.setPayload("Message Payload 1");
			msg1.setPublishTopicId((1l+(long)(Math.random()*3l))*10l);
			WebSocketPubSubBroker.publish(msg1);
		}
		logger.debug("Sent msgs in "+(System.currentTimeMillis()-startTime));
	}
}
