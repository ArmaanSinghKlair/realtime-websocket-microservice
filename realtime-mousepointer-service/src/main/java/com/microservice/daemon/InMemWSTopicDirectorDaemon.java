package com.microservice.daemon;

import java.util.ArrayDeque;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.microservice.pubsub.WebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.websocket.WebSocketMessageHandler;

/**
 * Responsible for directing messages stored in a topic to respective consumer queues to be consumed at their own pace.
 * Architecture: 1 daemon thread per topic.
 * 
 * * Messages are removed from topic queue once they've been sent to all the consumers.
 * * When syncing messages to a consumer, this daemon extracts SUBSCRIBER_FLUSH_BUFFER_LENGTH messages in a buffer and flushes the buffer altogether.
 *   This method ensures less overhead and increases throughput.
 */
@Component
public class InMemWSTopicDirectorDaemon {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	//Max length of items that can be enqueued to subscriber at ONCE.
	//If too low OR too high, it can cause latency issues (too low = consistently slow latency if many msgs, too high= slow if many messages in topicQueue)
	private static final int SUBSCRIBER_FLUSH_BUFFER_LENGTH = 100;
	
	/**
	 * Ideally 1 thread/topic for broadcasting messages to appropriate consumers
	 * @param topicQueue
	 * @param subscriberSet
	 * @param isTopicQueueNOTProcessing
	 * @param topic
	 */
	@Async
	public void flushTopicQueue(InMemoryWebSocketTopic topic) {
		try {
			while(true) {
				List<WebSocketMessage> messageBuffer = new ArrayList<>();
				while(messageBuffer.size() < SUBSCRIBER_FLUSH_BUFFER_LENGTH) {
					WebSocketMessage curMsg = null;
					synchronized(topic.getTopicQueue()) {
						curMsg = topic.getTopicQueue().poll();
					}
					if(curMsg == null) {
						break;	//no more items to add
					} else {
						messageBuffer.add(curMsg);
					}
				}
				if(messageBuffer.isEmpty()) {
					break;	//no more messages left to send
				}
				
				//Add buffer subscriber queue
				for(Long subscriberId: topic.getSubscriberSet()) {
					ArrayDeque<InMemoryWebSocketSubscriber> subscriberSockets = WebSocketMessageHandler.connectionBySubscriberMap.get(subscriberId);
					if(subscriberSockets != null) {
						for(InMemoryWebSocketSubscriber subscriberSocket: subscriberSockets) {
							subscriberSocket.enqueueNewMessages(messageBuffer);
						}
					}
				}
			}
		} catch(Exception e) {
			logger.error("Error in daemon: PubSubMsgDirectorDaemon",e);
		} finally {
			topic.getIsTopicNotProcessing().set(true);
			
			//Queue up processing if queue still has elements
			//topicQueue is critical
			synchronized(topic.getTopicQueue()) {
				if(!topic.getTopicQueue().isEmpty()) {
					topic.notifyMsgDirector(); 
				}
			}
		}
	}
}
