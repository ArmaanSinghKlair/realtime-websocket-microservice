package com.microservice.service;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import com.google.gson.JsonObject;
import com.microservice.pubsub.InMemoryWebSocketPubSubBroker;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.WebSocketMessage;
import com.microservice.pubsub.WebSocketMessage.WebSocketMessagePayload;
import com.microservice.util.JsonUtil;
import com.microservice.websocket.WebSocketMessageHandler;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)	//create new listener whenever autowired.
public class RedisStreamListener implements StreamListener<String, ObjectRecord<String, String>> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
    @Autowired
	InMemoryWebSocketPubSubBroker internalPubSubBroker;
	@Autowired
	ApplicationContext appContext;
    @Autowired
    RedisService redisService;
	/**
	 * Previous redis stream record id recieved in this listnere
	 */
	private String prevousPersistenceId;
	private String subscriberSocketId;
	private String subscriberId;
	private String topicId;
	
    @Override
    public void onMessage(ObjectRecord<String, String> record) {
    	try {
    		String streamMsgId = record.getId().getValue();
    		if(prevousPersistenceId != null && streamMsgId.equals(prevousPersistenceId)) {
    			return;	//probably double-sending OR sending after catchup.
    		}
			WebSocketMessage msg = JsonUtil.fromJson(record.getValue(), WebSocketMessage.class);
			msg.setPersistenceId(streamMsgId);
			msg.setPreviousPersistenceId(prevousPersistenceId);
			String topicId = msg.getTargetTopicId();
			
			//Unsubscribe from redis => IF NO topic found in this microservice instance
			Object topicLock = WebSocketMessageHandler.topicLockMap.computeIfAbsent(topicId, k -> new Object());
			synchronized(topicLock) {
				if(!WebSocketMessageHandler.topicMap.containsKey(topicId)) {
					//unsubscribe from this topic in redis
					redisService.unsubscribeFromStream(subscriberSocketId, topicId);
					return;
				}
			}
			
			//Unsubscribe, if socket closed
			InMemoryWebSocketSubscriber subscriberSocket = WebSocketMessageHandler.subscriberSocketMap.get(subscriberSocketId);
			if(subscriberSocket == null) {
				//remove unsubscribed user listener
				redisService.unsubscribeFromStream(subscriberSocketId, topicId);
				
				//Notify other subscribers of removal
				WebSocketMessage unsubMsg = new WebSocketMessage();
				unsubMsg.setTypeCd(WebSocketMessage.TYPE_CD_UNSUBSCRIBE);
				unsubMsg.setPersistentMsgCd(WebSocketMessage.PERSISTENT_MSG_CD_YES);
				unsubMsg.setTargetTopicId(topicId);
				{
					WebSocketMessage.WebSocketMessagePayload unsubMsgPayload = unsubMsg.new WebSocketMessagePayload();
					unsubMsgPayload.setTypeCd(WebSocketMessagePayload.TYPE_CD_USER_DISCONNECTED);
				
					JsonObject unsubPayloadJson = new JsonObject();
					unsubPayloadJson.addProperty("userId", subscriberId);
					unsubMsgPayload.setPayloadValue(unsubPayloadJson.toString());
					
					unsubMsg.setPayload(unsubMsgPayload);
				}
				redisService.produceStreamRecord(msg.getTargetTopicId(), JsonUtil.toJson(unsubMsg));
				return;
			}
			
			//if no issues, enqueue messages in subscriber message queue.
			subscriberSocket.enqueueNewMessages(Arrays.asList(msg));
			prevousPersistenceId = streamMsgId;			
		} catch(Exception e) {
			logger.error("Got error while processing message: " + record.toString(), e);
		}
    }
	public String getPrevousPersistenceId() {
		return prevousPersistenceId;
	}
	public void setPrevousPersistenceId(String prevousPersistenceId) {
		this.prevousPersistenceId = prevousPersistenceId;
	}
	public String getSubscriberSocketId() {
		return subscriberSocketId;
	}
	public void setSubscriberSocketId(String subscriberSocketId) {
		this.subscriberSocketId = subscriberSocketId;
	}
	public String getSubscriberId() {
		return subscriberId;
	}
	public void setSubscriberId(String subscriberId) {
		this.subscriberId = subscriberId;
	}
	public String getTopicId() {
		return topicId;
	}
	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}
}