package com.microservice.pubsub;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import com.microservice.contract.PubSubWebSocketSubscriberInterface;
import com.microservice.daemon.InMemWSSubscriberDirectorDaemon;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)	//create new topic whenever autowired
public class InMemoryWebSocketSubscriber implements PubSubWebSocketSubscriberInterface{
	@Autowired
	InMemWSSubscriberDirectorDaemon subscriberDaemon;
	
	private WebSocketSession websocketSession;
	private Long subscriberId;
	
	private LocalDateTime lastPingReceiveTime;
	/**
     * Is any daemon currently NOT processing topicQueue
     */
    public AtomicBoolean isSubscriberNotProcessing = new AtomicBoolean(true);
	/**
	 * ONLY FOR TESTING PURPOSES. remove IF NOT NEEDED
	 */
	@Deprecated
	public LocalDateTime lastTestTime;
	/**
	 * Push-based delivery. Subscriber can handle consumption of messages at its own pace. Decouples from producer
	 */
	private ArrayDeque<WebSocketMessage> messageQueue = new ArrayDeque<>();

	@Override
	public void enqueueNewMessages(List<WebSocketMessage> newMessageList) {
//		if(!websocketSession.isOpen()) {
//			//cleanup subscriber
//		}
		synchronized(messageQueue) {
			for(WebSocketMessage newMsg : newMessageList) {
				if(newMsg.getCreateSubscriberId().equals(this.subscriberId)) {
					continue;	//don't send my own messages to myself
				}
				messageQueue.offer(newMsg);
			}
		}
		notifyMsgFlushDaemon();
		lastTestTime = LocalDateTime.now();
	}
	
	/**
	 * PubSubMsgDirectorDaemon handles messages that are NOT URGENT.
	 * Following code ensures ONLY 1 instance of topicId is present in queue of msg director. 
	 */
	public void notifyMsgFlushDaemon() {
		//start broadcasting messages if not already processing
		if(isSubscriberNotProcessing.compareAndExchangeRelease(true, false)) {
			try {
				subscriberDaemon.flushSubscriberQueue(this);
			} catch(Exception e) {
				//task rejection errors
				isSubscriberNotProcessing.set(true);	//clear sempahore
			}
		}
	}
	
	public AtomicBoolean getIsSubscriberNotProcessing() {
		return isSubscriberNotProcessing;
	}

	public WebSocketSession getWebsocketSession() {
		return websocketSession;
	}
	public void setWebsocketSession(WebSocketSession websocketSession) {
		this.websocketSession = websocketSession;
	}
	public Long getSubscriberId() {
		return subscriberId;
	}
	public void setSubscriberId(Long subscriberId) {
		this.subscriberId = subscriberId;
	}
	public ArrayDeque<WebSocketMessage> getMessageQueue() {
		return messageQueue;
	}
	public LocalDateTime getLastPingReceiveTime() {
		return lastPingReceiveTime;
	}
	public void setLastPingReceiveTime(LocalDateTime lastPingRecieveTime) {
		this.lastPingReceiveTime = lastPingRecieveTime;
	}
}
