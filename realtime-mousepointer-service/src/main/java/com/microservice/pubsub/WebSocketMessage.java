package com.microservice.pubsub;

public class WebSocketMessage {
	//Business messages
	//Initial topic subscription is handled on connection creation. Later on subscriptions might be for something else
	public static final int TYPE_CD_PUBLISH = 0;
	public static final int TYPE_CD_SUBSCRIBE = 1;
	
	//Connection maintenance messages
	public static final int TYPE_CD_PING = 100;
	public static final int TYPE_CD_PONG = 101;
	
	private String persistenceId;	//helpful for keeping track of messages stored in redis streams
	private String prevousPersistenceId;	//prevous persistence message id for this websocket
	private Integer typeCd;
	private Integer payloadTypeCd;	
	private WebSocketMessagePayload payload;
	private Long createTimeUtcMs;
	private Integer timezoneOffsetMins;
	private String createSubscriberId;
	/**
	 * UUID of WebSocketSession that created this message.
	 * Nice to have, but client's dont know about this ID. Helpful only for the system.
	 */
	private String createSubscriberSocketId;
	
	//Used when subscribing to topic
	private String subscribeTopicId;
	private Integer subscribeTopicPersistentMessagingCd;
	
	//Used when publishing messages to topic
	private String publishTopicId;
	
	public String getSubscribeTopicId() {
		return subscribeTopicId;
	}
	public void setSubscribeTopicId(String subscribeTopicId) {
		this.subscribeTopicId = subscribeTopicId;
	}
	public String getPublishTopicId() {
		return publishTopicId;
	}
	public void setPublishTopicId(String publishTopicId) {
		this.publishTopicId = publishTopicId;
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
	 * Payload properties different from websocket itself
	 */
	public class WebSocketMessagePayload{	
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
	public Integer getSubscribeTopicPersistentMessagingCd() {
		return subscribeTopicPersistentMessagingCd;
	}
	public void setSubscribeTopicPersistentMessagingCd(Integer subscribeTopicPersistentMessagingCd) {
		this.subscribeTopicPersistentMessagingCd = subscribeTopicPersistentMessagingCd;
	}
}
