package com.microservice;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.microservice.pubsub.InMemoryWebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketPubSubBroker;
import com.microservice.websocket.WebSocketMessageHandler;

@SpringBootApplication
public class RealtimeWebsocketMicroserviceApplication implements CommandLineRunner {
	private static final Logger logger = LoggerFactory.getLogger(RealtimeWebsocketMicroserviceApplication.class);
		
	public static void main(String[] args) {
		SpringApplication.run(RealtimeWebsocketMicroserviceApplication.class, args);

	}
	
	/**
	 * Used for testing
	 */
	@Override
	public void run(String... args) throws Exception {
//		testPubSub();
	}
	
	/**
	 * Sends messages 
	 */
	private void testPubSub() {
		InMemoryWebSocketSubscriber sub1 = new InMemoryWebSocketSubscriber();
		sub1.setLastCommunicationTime(LocalDateTime.now());
		sub1.setSubscriberId(1L);
		InMemoryWebSocketSubscriber sub2 = new InMemoryWebSocketSubscriber();
		sub2.setLastCommunicationTime(LocalDateTime.now());
		sub2.setSubscriberId(2L);
		InMemoryWebSocketSubscriber sub3 = new InMemoryWebSocketSubscriber();
		sub3.setLastCommunicationTime(LocalDateTime.now());
		sub3.setSubscriberId(3L);
		//now mimic connecting to websockets
		WebSocketMessageHandler.connectionBySubscriberMap.put(1l, new ArrayDeque<>(Arrays.asList(sub1)));
		WebSocketMessageHandler.connectionBySubscriberMap.put(2l, new ArrayDeque<>(Arrays.asList(sub2)));
		WebSocketMessageHandler.connectionBySubscriberMap.put(3l, new ArrayDeque<>(Arrays.asList(sub3)));
		
		//Creat topics
		InMemoryWebSocketTopic topic1 = new InMemoryWebSocketTopic();
		topic1.setName("Topic 1");
		topic1.setTopicId(10l);
		
		
		InMemoryWebSocketTopic topic2 = new InMemoryWebSocketTopic();
		topic2.setName("Topic 2");
		topic2.setTopicId(20l);
		
		
		InMemoryWebSocketTopic topic3 = new InMemoryWebSocketTopic();
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
		for(int i=0;i<10000000;i++) {
			InMemoryWebSocketMessage msg1 = new InMemoryWebSocketMessage();
			msg1.setCreateSubscriberId(1l);
			msg1.setPayload("Message Payload 1");
			msg1.setPublishTopicId((1l+(long)(Math.random()*3l))*10l);
			WebSocketPubSubBroker.publish(msg1);
		}
		logger.debug("Sent msgs in "+(System.currentTimeMillis()-startTime));
	}
	
}
