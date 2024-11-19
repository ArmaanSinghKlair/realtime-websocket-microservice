package com.microservice.daemon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;

import com.microservice.pubsub.WebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.util.JsonUtil;

/**
 * Responsible for flushing messages to the websocket one-by-one. NOTE: There can be multiple InMemoryWebSocketSubscriber / per subscriberId.
 * This happens when the same subscriberId connects via multiple browser tabs/devices
 * 
 * Architecture: 1 daemon thread per InMemoryWebSocketSubscriber.
 * 
 * * Messages are removed from subscriber queue once they've been flushed.
 */
@Component
public class InMemWSSubscriberDirectorDaemon {
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
				WebSocketMessage curMsg = null;
				synchronized (subscriber.getMessageQueue()) {
					curMsg = subscriber.getMessageQueue().poll();
				}
				if(curMsg == null) {
					break;	//no more items to process
				}
				if(curMsg.getCreateSubscriberId().equals(subscriber.getSubscriberId())) {
					continue;	//don't send to message creator
				}
				if(subscriber.getWebsocketSession().isOpen()) {
					subscriber.getWebsocketSession().sendMessage(new TextMessage(JsonUtil.toJson(curMsg)));
				}
			}
		} catch(Exception e) {
			logger.error("Error in daemon: PubSubMsgDirectorDaemon",e);
		} finally {
			subscriber.getIsSubscriberNotProcessing().set(true);
			
			//Queue up processing if queue still has elements
			//topicQueue is critical
			synchronized(subscriber.getMessageQueue()) {
				if(!subscriber.getMessageQueue().isEmpty()) {
					subscriber.notifyMsgFlushDaemon(); 
				}
			}
		}
	}
}
