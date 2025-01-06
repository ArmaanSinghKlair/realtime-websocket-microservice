package com.microservice.pubsub;

import com.microservice.util.DateUtil;

/**
 * Keep in sync with Javascript class with same name.
 */
public class WebSocketMessage {
	//Business messages
	//Initial topic subscription is handled on connection creation. Later on subscriptions might be for something else
	public static final int TYPE_CD_PUBLISH = 0;
	public static final int TYPE_CD_SUBSCRIBE = 1;
	public static final int TYPE_CD_UNSUBSCRIBE = 2;
	
	//Connection maintenance messages
	public static final int TYPE_CD_PING = 100;
	public static final int TYPE_CD_PONG = 101;
	public static final int TYPE_CD_CATCHUP_REQUEST = 102;
	public static final int TYPE_CD_CATCHUP_COMPLETE = 103;
	
	public static final int PERSISTENT_MSG_CD_YES = 1;
	public static final int PERSISTENT_MSG_CD_NO = 0;
	
	private Integer typeCd;
	private Integer payloadTypeCd;	
	private WebSocketMessagePayload payload;
	private Long createTimeUtcMs;
	private Integer timezoneOffsetMins;
	private String createSubscriberId;
	private String persistenceId;	//helpful for keeping track of messages stored in redis streams
	private String prevousPersistenceId;	//prevous persistence message id for this websocket
	/**
	 * UUID of WebSocketSession that created this message.
	 * Nice to have, but client's dont know about this ID. Helpful only for the system.
	 */
	private String createSubscriberSocketId;
	private String targetTopicId;

	//Used when publishing to topic
	private Integer persistentMsgCd = PERSISTENT_MSG_CD_NO;
	
	public WebSocketMessage() {
		this.createTimeUtcMs = System.currentTimeMillis();
		this.timezoneOffsetMins = DateUtil.getSysTimezoneOffsetMinsJS();
	}
	
	public String getCreateSubscriberId() {
		return createSubscriberId;
	}
	public void setCreateSubscriberId(String createSubscriberId) {
		this.createSubscriberId = createSubscriberId;
	}
	public Integer getTypeCd() {
		return typeCd;
	}
	public void setTypeCd(Integer typeCd) {
		this.typeCd = typeCd;
	}
	public Integer getPayloadTypeCd() {
		return payloadTypeCd;
	}
	public void setPayloadTypeCd(Integer payloadTypeCd) {
		this.payloadTypeCd = payloadTypeCd;
	}	
	
	/**
	 * Payload properties different from websocket itself.
	 * Keep in sync with Javascript class with same name.
	 */
	public class WebSocketMessagePayload{	
		public static final int TYPE_CD_MOUSE_COORDINATES = 0;	// { x: 1, y: 2, userId: 123}
		public static final int TYPE_CD_CHAT_MESSAGE = 1;	// {userId: 123, groupId: 123, chatMessage: 'chat message'}
		public static final int TYPE_CD_USER_CONNECTED = 2;	//{userId:123, username: 'username'}
		public static final int TYPE_CD_USER_DISCONNECTED = 3;	//{userId: 123}
		
		private Integer typeCd;
		private String payloadValue;
		
		public Integer getTypeCd() {
			return typeCd;
		}
		public void setTypeCd(Integer typeCd) {
			this.typeCd = typeCd;
		}
		public String getPayloadValue() {
			return payloadValue;
		}
		public void setPayloadValue(String payloadValue) {
			this.payloadValue = payloadValue;
		}
	}

	public void setPayload(WebSocketMessagePayload payload) {
		this.payload = payload;
	}
	public Long getCreateTimeUtcMs() {
		return createTimeUtcMs;
	}
	public void setCreateTimeUtcMs(Long createTimeUtcMs) {
		this.createTimeUtcMs = createTimeUtcMs;
	}
	public WebSocketMessagePayload getPayload() {
		return payload;
	}
	public Integer getTimezoneOffsetMins() {
		return timezoneOffsetMins;
	}
	public void setTimezoneOffsetMins(Integer timezoneOffsetMins) {
		this.timezoneOffsetMins = timezoneOffsetMins;
	}
	public String getPersistenceId() {
		return persistenceId;
	}
	public void setPersistenceId(String persistenceId) {
		this.persistenceId = persistenceId;
	}
	public String getPrevousPersistenceId() {
		return prevousPersistenceId;
	}
	public void setPrevousPersistenceId(String prevousPersistenceId) {
		this.prevousPersistenceId = prevousPersistenceId;
	}
	public String getCreateSubscriberSocketId() {
		return createSubscriberSocketId;
	}
	public void setCreateSubscriberSocketId(String createSubscriberSocketId) {
		this.createSubscriberSocketId = createSubscriberSocketId;
	}
	public String getTargetTopicId() {
		return targetTopicId;
	}
	public void setTargetTopicId(String targetTopicId) {
		this.targetTopicId = targetTopicId;
	}
	public Integer getPersistentMsgCd() {
		return persistentMsgCd;
	}
	public void setPersistentMsgCd(Integer persistentMsgCd) {
		this.persistentMsgCd = persistentMsgCd;
	}
}
