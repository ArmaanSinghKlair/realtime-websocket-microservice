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

import com.microservice.daemon.InMemWSTopicDirectorDaemon;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketMessage;
import com.microservice.pubsub.WebSocketPubSubBroker;
import com.microservice.websocket.WebSocketMessageHandler;

@Component
public class PubSubBrokerTest implements CommandLineRunner{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	@Autowired
	InMemWSTopicDirectorDaemon topicDaemon;
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
		WebSocketPubSubBroker.createTopic(topic1);
	}
	
	/**
	 * Sends messages 
	 */
	@Deprecated
	private void testPubSub() {
		long topicNum = 500l;
		long subscriberNum=500;
		int messagesNum=1000000;
		
		//Creat topics
		for(long i=1;i<=topicNum;i++) {
			InMemoryWebSocketTopic topic = appContext.getBean(InMemoryWebSocketTopic.class);
			topic.setName("Topic "+i);
			topic.setTopicId(i);
			WebSocketPubSubBroker.createTopic(topic);	//register with broker
		}
		
		//Create Subscriber
		for(long i=1;i<=subscriberNum;i++) {
			InMemoryWebSocketSubscriber sub = appContext.getBean(InMemoryWebSocketSubscriber.class);
			sub.setLastPingReceiveTime(LocalDateTime.now());
			sub.setSubscriberId(i);
			//register with broker
			WebSocketMessageHandler.connectionBySubscriberMap.put(i, new ArrayDeque<>(Arrays.asList(sub)));
			
			//Now subscriber to topics
			for(long j=1;j<=topicNum;j++) {
				WebSocketPubSubBroker.subscribeToTopic(j, i);
			}
		}

		Long startTime = System.currentTimeMillis();
		logger.debug("Starting to send msgs");
		//send messages
		for(int i=0;i<messagesNum;i++) {
			WebSocketMessage msg1 = new WebSocketMessage();
			msg1.setCreateSubscriberId(1l);
			msg1.setPublishTopicId(1l+(long)(Math.random()*topicNum));
			
			WebSocketMessage.WebSocketMessagePayload payload = msg1.new WebSocketMessagePayload();
			payload.setPayloadValue("Message Payload 1");
			msg1.setPayload(payload);
			WebSocketPubSubBroker.publish(msg1);
		}
		logger.debug("Sent msgs in "+(System.currentTimeMillis()-startTime));
	}
}
