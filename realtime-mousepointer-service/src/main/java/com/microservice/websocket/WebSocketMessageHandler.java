package com.microservice.websocket;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;
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
import org.springframework.util.ObjectUtils;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator.OverflowStrategy;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.gson.JsonObject;
import com.microservice.pubsub.InMemoryWebSocketPubSubBroker;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketMessage;
import com.microservice.pubsub.WebSocketMessage.WebSocketMessagePayload;
import com.microservice.service.RedisService;
import com.microservice.spring.RedisConfig;
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
	RedisService redisService;
	@Autowired
	InMemoryWebSocketPubSubBroker internalPubSubBroker;
	
	/**
	 * Time limit (milliseconds) to send messages in Websocket
	 */
    private static final int SOCKET_SEND_TIME_LIMIT = 5000;

    /**
     * Maximum Websocket Buffer size in bytes
     */
    private static final int SOCKET_BUFFER_SIZE_LIMIT = 1000 * 1024; // 1 MB
    
    /**
     * Key for WebSocketSession.getAttributes map. Value is a thread safe decorator for WebSocketSession
     */
    public static final String THREAD_SAFE_SOCKET_KEY = "THREAD_SAFE_SOCKET_KEY";
    
	/**
	 *  <WebSocket ID, Subscriber Info>.
	 */
	public static ConcurrentHashMap<String, InMemoryWebSocketSubscriber> subscriberSocketMap = new ConcurrentHashMap<>();
	
	/**
	 * <Topic ID, Topic itself>
	 * TODO: clear this when a topic closes OR when no subscribers are in this topic. This will trigger Redis topicId listening cleanup as well.
	 * <TopicId, Topic>
	 */
	public static ConcurrentHashMap<String, InMemoryWebSocketTopic> topicMap = new ConcurrentHashMap<>();
	public static final Map<String, Object> topicLockMap = new ConcurrentHashMap<>();
	
	/**
	 * <subscriberSocketId, List<TopicId>>
	 * List of topics, user has subscribed to.
	 */
	public static ConcurrentHashMap<String, ArrayDeque<String>> subscriberTopicListMap = new ConcurrentHashMap<>();
	public static final Map<String, Object> subscriberTopicListLockMap = new ConcurrentHashMap<>();
	
	@Override
	public void handleTextMessage(WebSocketSession session, TextMessage message)
			throws InterruptedException, IOException {
		try {
			WebSocketMessage msg = JsonUtil.fromJson(message.getPayload(), WebSocketMessage.class);
			InMemoryWebSocketSubscriber subscriber = subscriberSocketMap.get(session.getId());
			
			//Helpful info for system
			String subscriberSocketId = subscriber.getThreadSafeWebSocketSession().getId();
			msg.setCreateSubscriberSocketId(subscriberSocketId);
			String targetTopicId = msg.getTargetTopicId();
			
			boolean isPersistentMsg = msg.getPersistentMsgCd().equals(WebSocketMessage.PERSISTENT_MSG_CD_YES);
			switch(msg.getTypeCd()) {
				case WebSocketMessage.TYPE_CD_PUBLISH:							
					if(isPersistentMsg) {
						//REDIS STREAMs (persistent messages)
						redisService.produceStreamRecord(targetTopicId, JsonUtil.toJson(msg));
					} else {
						//Low priority = REDIS PUB-SUB (might lose messages if instance/(socket on that instance) not connected)
						redisService.publish(targetTopicId, JsonUtil.toJson(msg));	
					}
					
				break;
				case WebSocketMessage.TYPE_CD_SUBSCRIBE:
					//subscribe this user to topicId (within local pub-sub)
					internalPubSubBroker.subscribeToTopic(targetTopicId, msg.getCreateSubscriberSocketId(), appContext);
						
					//subscribe this inter-microservice pub-sub to specific topicId (if not already)
					//Redis STREAMS
					//At-least-once delivery
					redisService.subscriberToStream(subscriberSocketId, subscriber.getSubscriberId(), targetTopicId, null);

					//Redis PUB-SUB 
					//At-most-once delivery
					Object redisTopicLock = RedisConfig.redisPubSubTopicLockMap.computeIfAbsent(targetTopicId,k ->new Object());
					//avoid duplicate subscriptions
					synchronized(redisTopicLock) {
						if(!RedisConfig.redisPubSubTopicMap.containsKey(targetTopicId)) {							
							//subscribe this microservice to topicId (for inter-microservice pub-sub)
							//In some cases, subscribers for 1 topic get connected to multiple microservice (due to horizontal scaling). We need to send messages to ALL microservices having this particular topicId
							ChannelTopic topic = new ChannelTopic(targetTopicId);
							redisContainer.addMessageListener(redisPubSubListener, topic);
							RedisConfig.redisPubSubTopicMap.put(targetTopicId, topic); //add to cache
						}
					}
					
					//inform other listeners of user subscription
					if(isPersistentMsg) {
						//REDIS STREAMs (persistent messages)
						redisService.produceStreamRecord(targetTopicId, JsonUtil.toJson(msg));
					} else {
						//Low priority = REDIS PUB-SUB (might lose messages if instance/(socket on that instance) not connected)
						redisService.publish(targetTopicId, JsonUtil.toJson(msg));	
					}
					
					synchronized(subscriberTopicListLockMap.computeIfAbsent(subscriberSocketId, s->new Object())) {
						subscriberTopicListMap.computeIfAbsent(subscriberSocketId, s->new ArrayDeque<>());
						subscriberTopicListMap.get(subscriberSocketId).add(targetTopicId);
					}
				break;
				case WebSocketMessage.TYPE_CD_PING:	
//					logger.debug("Got Ping from Subscriber Id: "+subscriber.getSubscriberId());
					subscriber.setLastPingReceiveTime(LocalDateTime.now());
					
					//return a pong message
					WebSocketMessage returnMsg = new WebSocketMessage();
					returnMsg.setTypeCd(WebSocketMessage.TYPE_CD_PONG);
					
					//Send message directory to socket. Cannot use pub-sub since NO topic associated here.
					if(subscriber.getThreadSafeWebSocketSession().isOpen()){
						subscriber.enqueueNewMessages(Arrays.asList(returnMsg));
					}
				break;
				case WebSocketMessage.TYPE_CD_CATCHUP_REQUEST:
					//remove current subscrption for this socket
					redisService.unsubscribeFromStream(subscriberSocketId, targetTopicId);
					
					//Tell user to start listening for persistent messages again
					//Send message directory to socket. Cannot use pub-sub since NO topic associated here.
					WebSocketMessage catchupCompleteMsg = new WebSocketMessage();
					catchupCompleteMsg.setTypeCd(WebSocketMessage.TYPE_CD_CATCHUP_COMPLETE);
					if(subscriber.getThreadSafeWebSocketSession().isOpen()){
						subscriber.enqueueNewMessages(Arrays.asList(catchupCompleteMsg));
					}
					
					//Resubsribe to persistent msgs after some time
					Thread.sleep(1000);	//allow user to recevie msgs because we don't have acknowledge mechanisms for now
					redisService.subscriberToStream(subscriberSocketId, subscriber.getSubscriberId(), targetTopicId, msg.getPreviousPersistenceId());
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
			String userId = queryParams.getFirst("userId");
			if(ObjectUtils.isEmpty(userId)) {
				throw new RuntimeException("userId Empty");
			}
			
			//TODO JWT auth here
			ConcurrentWebSocketSessionDecorator threadSafeWebSocketSession = new ConcurrentWebSocketSessionDecorator(session, SOCKET_SEND_TIME_LIMIT, SOCKET_BUFFER_SIZE_LIMIT, OverflowStrategy.DROP);
			session.getAttributes().put(THREAD_SAFE_SOCKET_KEY, session);
			InMemoryWebSocketSubscriber subscriber = appContext.getBean(InMemoryWebSocketSubscriber.class);
			subscriber.setLastPingReceiveTime(LocalDateTime.now());
			subscriber.setSubscriberId(userId);
			subscriber.setThreadSafeWebSocketSession(threadSafeWebSocketSession);
			
			subscriberSocketMap.put(threadSafeWebSocketSession.getId(), subscriber);
			
			logger.debug("CREATING websocket session for subscriberID: "+subscriber.getSubscriberId() + ", subscriberSocketId: "+subscriber.getThreadSafeWebSocketSession().getId()+" (Remote Address: "+ session.getRemoteAddress().toString()+")");
			
		} catch(Exception e) {
			session.close();
			logger.error("Error afterConnectionEstablished",e);
		}
		
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		super.afterConnectionClosed(session, status);
		
		String subscriberSocketId = session.getId();
		InMemoryWebSocketSubscriber subscriber = subscriberSocketMap.remove(subscriberSocketId);
		
		synchronized(subscriberTopicListLockMap.computeIfAbsent(subscriberSocketId, s->new Object())) {
			if(subscriberTopicListMap.containsKey(subscriberSocketId)) {
				for(String topicId: subscriberTopicListMap.get(subscriberSocketId)) {
					//Notify other subscribers of removal
					WebSocketMessage unsubMsg = new WebSocketMessage();
					unsubMsg.setTypeCd(WebSocketMessage.TYPE_CD_UNSUBSCRIBE);
					unsubMsg.setPersistentMsgCd(WebSocketMessage.PERSISTENT_MSG_CD_YES);
					unsubMsg.setTargetTopicId(topicId);
					{
						WebSocketMessage.WebSocketMessagePayload unsubMsgPayload = unsubMsg.new WebSocketMessagePayload();
						unsubMsgPayload.setTypeCd(WebSocketMessagePayload.TYPE_CD_USER_DISCONNECTED);
					
						JsonObject unsubPayloadJson = new JsonObject();
						unsubPayloadJson.addProperty("userId", subscriber.getSubscriberId());
						unsubMsgPayload.setPayloadValue(unsubPayloadJson.toString());
						
						unsubMsg.setPayload(unsubMsgPayload);
					}
					redisService.produceStreamRecord(topicId, JsonUtil.toJson(unsubMsg));
				}
			}
		}
		synchronized (subscriberTopicListMap.getOrDefault(subscriberSocketId, new ArrayDeque<>())) {
			
			
		}
		if(subscriber == null) {
			logger.debug("REMOVING websocket session for (NA subscriberSocketId, Couldn't find InMemoryWebSocketSubscriber...weird), (Remote Address: "+ session.getRemoteAddress().toString()+"), (Close Status: "+status.toString()+")");
		} else {
			logger.debug("REMOVING websocket session for (subscriberId: "+subscriber.getSubscriberId() + ", subscriberSocketId: "+subscriber.getThreadSafeWebSocketSession().getId()+"), (Remote Address: "+ session.getRemoteAddress().toString()+"), (Close Status: "+status.toString()+")");
		}
	}
}
