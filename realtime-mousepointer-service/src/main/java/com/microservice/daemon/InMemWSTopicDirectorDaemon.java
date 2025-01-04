package com.microservice.daemon;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketMessage;
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
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
				synchronized(topic.getSubscriberSocketSet()) {
					for(String subscriberSocketId: topic.getSubscriberSocketSet()) {
						
						InMemoryWebSocketSubscriber subscriberSocket = WebSocketMessageHandler.subscriberSocketMap.get(subscriberSocketId);
						if(subscriberSocket != null) {
							subscriberSocket.enqueueNewMessages(messageBuffer);
						} else {
							//CLEANUP topic subscribers, if web socket not found.
							//CAUSES: Subscriber disconnected OR subscriber is registered on some OTHER microservice instance.
							topic.getSubscriberSocketSet().remove(subscriberSocketId);
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
