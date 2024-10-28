package com.microservice.websocket;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import com.microservice.pubsub.InMemoryWebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.WebSocketPubSubBroker;
import com.microservice.util.JsonUtil;

@Component
public class WebSocketMessageHandler extends TextWebSocketHandler{
	private static final Logger logger = LoggerFactory.getLogger(WebSocketMessageHandler.class);
	
	//register this cache to be cleaned up hourly
//	static CacheCleanupConfigBuilder<Long,Boolean> ttlCacheConfigBuilder = new CacheCleanupConfigBuilder<Long,Boolean>();
//	static {
//		ttlCacheConfigBuilder.setExpireMins(5); //entries older than 5 mins should be cleaned up
//		ttlCacheConfigBuilder.setExpireStrategy(new TTLCacheExpireStrategy<Long, Boolean>());
//	}
	/**
	 * Store websocket connections by userID.
	 * In context of pub-sub, these are ALL Consumers & Publishers.
	 */
	public static ConcurrentHashMap<Long, ArrayDeque<InMemoryWebSocketSubscriber>> connectionBySubscriberMap = new ConcurrentHashMap<>();
	//to get userId from websocketsession
	public static ConcurrentHashMap<WebSocketSession, InMemoryWebSocketSubscriber> webSocketSubscriberMap = new ConcurrentHashMap<>();
	@Override
	public void handleTextMessage(WebSocketSession session, TextMessage message)
			throws InterruptedException, IOException {
		try {
			InMemoryWebSocketMessage msg = JsonUtil.fromJson(message.getPayload(), InMemoryWebSocketMessage.class);
			InMemoryWebSocketSubscriber subscriber = webSocketSubscriberMap.get(session);
			
			switch(msg.getTypeCd()) {
				case InMemoryWebSocketMessage.TYPE_CD_PUBLISH:
					WebSocketPubSubBroker.publish(msg);
				break;
				case InMemoryWebSocketMessage.TYPE_CD_SUBSCRIBE:
					WebSocketPubSubBroker.subscribeToTopic(msg.getSubscribeTopicId(), msg.getCreateSubscriberId());
				break;
				case InMemoryWebSocketMessage.TYPE_CD_PING:	
					subscriber.setLastPingReceiveTime(LocalDateTime.now());
					InMemoryWebSocketMessage returnMsg = new InMemoryWebSocketMessage();
					returnMsg.setTypeCd(InMemoryWebSocketMessage.TYPE_CD_PONG);
					returnMsg.setCreateUTCTimestamp(ZonedDateTime.now());
					session.sendMessage(new TextMessage(JsonUtil.toJson(returnMsg)));
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
			InMemoryWebSocketSubscriber subscriber = new InMemoryWebSocketSubscriber();
			subscriber.setLastPingReceiveTime(LocalDateTime.now());
			subscriber.setSubscriberId(userId);
			subscriber.setWebsocketSession(session);
			connectionBySubscriberMap.computeIfAbsent(userId, k -> new ArrayDeque<>());
			connectionBySubscriberMap.get(userId).add(subscriber);
			webSocketSubscriberMap.put(session, subscriber);
			
		} catch(Exception e) {
			session.close();
			logger.error("Error afterConnectionEstablished",e);
		}
		
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		super.afterConnectionClosed(session, status);
//		pmsUtilitySocketProcessor.removeWebSocketSession(session);		
	}
}
