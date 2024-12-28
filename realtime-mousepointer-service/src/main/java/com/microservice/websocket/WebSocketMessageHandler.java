package com.microservice.websocket;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import com.microservice.pubsub.InMemoryWebSocketPubSubBroker;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketMessage;
import com.microservice.service.RedisPubSubPublisherService;
import com.microservice.spring.RedisConfig;
import com.microservice.util.DateUtil;
import com.microservice.util.JsonUtil;

@Component
public class WebSocketMessageHandler extends TextWebSocketHandler{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	@Autowired
	ApplicationContext appContext;
	@Autowired
	RedisMessageListenerContainer redisContainer;
	@Autowired
	MessageListenerAdapter redisPubSubListener;
	@Autowired
	RedisPubSubPublisherService redisPubSubMsgPublisher;
	@Autowired
	InMemoryWebSocketPubSubBroker internalPubSubBroker;
	
	/**
	 * Store websocket connections by userID.
	 * In context of pub-sub, these are ALL Consumers & Publishers.
	 */
	public static ConcurrentHashMap<Long, ArrayDeque<InMemoryWebSocketSubscriber>> connectionBySubscriberMap = new ConcurrentHashMap<>();
	
	/**
	 * Helps high-througput Thread-safety. WebSocketMessageHandler.connectionBySubscriberMap is used extensively. 
	 * We don't want to lock entire map, only ensure operations on the subscriberId are atomic
	 */
	public static final ConcurrentHashMap<Long, Object> connectionBySubscriberLockMap = new ConcurrentHashMap<>();
	
	/**
	 *  Provides associated Subscriber Information via WebSocketSession object
	 */
	public static ConcurrentHashMap<WebSocketSession, InMemoryWebSocketSubscriber> webSocketSubscriberMap = new ConcurrentHashMap<>();
	
	/**
	 * Map of all topics by Id in this microservice instance
	 * TODO: clear this when a topic closes OR when no subscribers are in this topic. This will trigger Redis topicId listening cleanup as well.
	 * <TopicId, Topic>
	 */
	public static ConcurrentHashMap<Long, InMemoryWebSocketTopic> topicMap = new ConcurrentHashMap<>();
	public static final Map<Long, Object> topicLockMap = new ConcurrentHashMap<>();
	
	@Override
	public void handleTextMessage(WebSocketSession session, TextMessage message)
			throws InterruptedException, IOException {
		try {
			WebSocketMessage msg = JsonUtil.fromJson(message.getPayload(), WebSocketMessage.class);
			InMemoryWebSocketSubscriber subscriber = webSocketSubscriberMap.get(session);
			
			switch(msg.getTypeCd()) {
				case WebSocketMessage.TYPE_CD_PUBLISH:		
					long topicId = msg.getPublishTopicId();
					//publish to users connected on this instance.
					//Also, clears WebSocketPubSubBroker.topicLockMap if noone subscribed to topic
					internalPubSubBroker.publish(msg);
					
					if(msg.getPriorityCd().equals(WebSocketMessage.PRIORITY_CD_HIGH)) {
						//TODO
						//probably offer deduplication/durability
						//TODO write to redis streams/pub-sub depending upon priority
					} else {
						//Low priority = REDIS PUB-SUB to other microservices listening for this topicId.
						//Can afford to lose msgs here
						
						Object topicLock = topicLockMap.computeIfAbsent(topicId, k -> new Object());
						synchronized(topicLock) {
							if(topicMap.containsKey(topicId)) {
								redisPubSubMsgPublisher.publish(""+topicId, JsonUtil.toJson(msg));	
							} else {
								//unsubscribe from this topic in redis
								Object redisTopicLock = RedisConfig.redisPubSubTopicLockMap.computeIfAbsent(topicId, k ->new Object());
								//avoid duplicate subscriptions
								synchronized(redisTopicLock) {
									redisContainer.removeMessageListener(redisPubSubListener, RedisConfig.redisPubSubTopicMap.get(topicId));
									RedisConfig.redisPubSubTopicMap.remove(topicId);
								}
							}
						}
					}
					
				break;
				case WebSocketMessage.TYPE_CD_SUBSCRIBE:
					//subscribe this user to topicId (within local pub-sub)
					internalPubSubBroker.subscribeToTopic(msg.getSubscribeTopicId(), msg.getCreateSubscriberId(), appContext);
						
					//subscribe this inter-microservice pub-sub to specific topicId (if not already)
					Object redisTopicLock = RedisConfig.redisPubSubTopicLockMap.computeIfAbsent(msg.getSubscribeTopicId(),s->new Object());
					//avoid duplicate subscriptions
					synchronized(redisTopicLock) {
						if(!RedisConfig.redisPubSubTopicMap.containsKey(msg.getSubscribeTopicId())) {							
							//subscribe this microservice to topicId (for inter-microservice pub-sub)
							//In some cases, subscribers for 1 topic get connected to multiple microservice (due to horizontal scaling). We need to send messages to ALL microservices having this particular topicId
							ChannelTopic topic = new ChannelTopic(""+msg.getSubscribeTopicId());
							redisContainer.addMessageListener(redisPubSubListener, topic);
							RedisConfig.redisPubSubTopicMap.put(msg.getSubscribeTopicId(), topic); //add to cache
						}
					}
					
				break;
				case WebSocketMessage.TYPE_CD_PING:	
					logger.debug("Got Ping from Subscriber Id: "+subscriber.getSubscriberId());
					subscriber.setLastPingReceiveTime(LocalDateTime.now());
					
					//return a pong message
					WebSocketMessage returnMsg = new WebSocketMessage();
					returnMsg.setTypeCd(WebSocketMessage.TYPE_CD_PONG);
					returnMsg.setCreateTimeUtcMs(System.currentTimeMillis());
					returnMsg.setTimezoneOffsetMins(DateUtil.getSysTimezoneOffsetMinsJS());
					if(session.isOpen()) {
						session.sendMessage(new TextMessage(JsonUtil.toJson(returnMsg)));
					}
				break;	
			}

		} catch (Exception e) {
			logger.error("Error while handleTextMessage", e);
		}
	}

	
//    static {
//    	//register this cache 
//    	ttlCacheConfigBuilder.setCache(newConnectionLockMap);
//    	HourlyProcessor.hourlyCacheCleanupList.add(ttlCacheConfigBuilder.build());
//    }
	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
		try {
			MultiValueMap<String,String> queryParams = UriComponentsBuilder.fromUri(session.getUri()).build().getQueryParams();
			Long userId = Long.parseLong(queryParams.getFirst("userId"));
			//TODO JWT auth here
			InMemoryWebSocketSubscriber subscriber = appContext.getBean(InMemoryWebSocketSubscriber.class);
			subscriber.setLastPingReceiveTime(LocalDateTime.now());
			subscriber.setSubscriberId(userId);
			subscriber.setWebsocketSession(session);
			
			
			Object subscriberIdLock = connectionBySubscriberLockMap.computeIfAbsent(subscriber.getSubscriberId(),k->new Object());
			//ensure atomic operation for WebSocketMessageHandler.connectionBySubscriberMap
			synchronized(subscriberIdLock){
				connectionBySubscriberMap.computeIfAbsent(userId, k -> new ArrayDeque<>());
				connectionBySubscriberMap.get(userId).add(subscriber);
			}
			webSocketSubscriberMap.put(session, subscriber);
			
			logger.debug("CREATING websocket session for subscriberID: "+subscriber.getSubscriberId() + "(Remote Address: "+ session.getRemoteAddress().toString()+")");
			
		} catch(Exception e) {
			session.close();
			logger.error("Error afterConnectionEstablished",e);
		}
		
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		super.afterConnectionClosed(session, status);
					
		InMemoryWebSocketSubscriber subscriber = webSocketSubscriberMap.remove(session);
		if(subscriber == null) {
			logger.debug("REMOVING websocket session for (NA subscriberId, Couldn't find subscriberId...weird), (Remote Address: "+ session.getRemoteAddress().toString()+"), (Close Status: "+status.toString()+")");
		} else {
			logger.debug("REMOVING websocket session for (subscriberId: "+subscriber.getSubscriberId()+"), (Remote Address: "+ session.getRemoteAddress().toString()+"), (Close Status: "+status.toString()+")");
			
			Object subscriberIdLock = connectionBySubscriberLockMap.computeIfAbsent(subscriber.getSubscriberId(),s->new Object());
			//ensure atomic operation for WebSocketMessageHandler.connectionBySubscriberMap
			synchronized(subscriberIdLock) {
				ArrayDeque<InMemoryWebSocketSubscriber> subscriberSockets = WebSocketMessageHandler.connectionBySubscriberMap.get(subscriber.getSubscriberId());
				if(subscriberSockets != null) {
					subscriberSockets.remove(subscriber);
					//cleanup connectionBySubscriberMap if NO sockets left.
					if(subscriberSockets.isEmpty()) {
						connectionBySubscriberMap.remove(subscriber.getSubscriberId());
					}
				}
			}
		}
	}
}
