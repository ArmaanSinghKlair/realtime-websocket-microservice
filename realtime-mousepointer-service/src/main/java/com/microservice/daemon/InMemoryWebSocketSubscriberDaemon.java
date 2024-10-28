package com.microservice.daemon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.microservice.pubsub.InMemoryWebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;

@Component
public class InMemoryWebSocketSubscriberDaemon {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
		
	/**
	 * Ideally 1 thread/topic for broadcasting messages to appropriate consumers
	 * @param topicQueue
	 * @param subscriberSet
	 * @param isTopicQueueNOTProcessing
	 * @param topic
	 */
	@Async
	public void flushSubscriberQueue(InMemoryWebSocketSubscriber subscriber) {
		try {
			while(true) {
				InMemoryWebSocketMessage curMsg = null;
				synchronized (subscriber.getMessageQueue()) {
					curMsg = subscriber.getMessageQueue().poll();
				}
				if(curMsg == null) {
					break;	//no more items to process
				}
				if(curMsg.getCreateSubscriberId().equals(subscriber.getSubscriberId())) {
					continue;	//don't send if same create=subscriber
				}
//				if(subscriber.getWebsocketSession().isOpen()) {
//					subscriber.getWebsocketSession().sendMessage(new TextMessage(JsonUtil.toJson(curMsg)));
//				}
			}
		} catch(Exception e) {
			logger.error("Error in daemon: PubSubMsgDirectorDaemon",e);
		} finally {
			subscriber.getIsSubscriberNotProcessing().set(true);
			
			//Queue up processing if queue still has elements
			//topicQueue is critical
			synchronized(subscriber.getMessageQueue()) {
				boolean isTopicQueueEmpty = subscriber.getMessageQueue().isEmpty();
				if(!isTopicQueueEmpty) {
					subscriber.notifyMsgFlushDaemon(); 
				}
			}
		}
	}
}
