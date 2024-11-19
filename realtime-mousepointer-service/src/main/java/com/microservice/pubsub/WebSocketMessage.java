package com.microservice.pubsub;

import java.time.ZonedDateTime;

public class WebSocketMessage {
	//Business messages
	//Initial topic subscription is handled on connection creation. Later on subscriptions might be for something else
	public static final int TYPE_CD_PUBLISH = 0;
	public static final int TYPE_CD_SUBSCRIBE = 1;
	
	//Connection maintenance messages
	public static final int TYPE_CD_PING = 100;
	public static final int TYPE_CD_PONG = 101;
	
	public static final int PRIORITY_CD_HIGH = 1;	//messages need durability in-case of system failures
	public static final int PRIORITY_CD_LOW = 0;
	
	private Integer typeCd;
	private Integer payloadTypeCd;	
	private Integer priorityCd = PRIORITY_CD_LOW;	//whether messages needs to be durable OR NOT
	private WebSocketMessagePayload payload;
	private ZonedDateTime createUTCTimestamp;
	private Integer timezoneOffsetMins;
	private Long createSubscriberId;
	private Long subscribeTopicId;
	private Long publishTopicId;
	
	public Long getSubscribeTopicId() {
		return subscribeTopicId;
	}
	public void setSubscribeTopicId(Long subscribeTopicId) {
		this.subscribeTopicId = subscribeTopicId;
	}
	public Long getPublishTopicId() {
		return publishTopicId;
	}
	public void setPublishTopicId(Long publishTopicId) {
		this.publishTopicId = publishTopicId;
	}
	public Long getCreateSubscriberId() {
		return createSubscriberId;
	}
	public void setCreateSubscriberId(Long createSubscriberId) {
		this.createSubscriberId = createSubscriberId;
	}
	public Integer getTimezoneOffsetMins() {
		return timezoneOffsetMins;
	}
	public void setTimezoneOffsetMins(Integer timezoneOffsetMins) {
		this.timezoneOffsetMins = timezoneOffsetMins;
	}
	public Integer getTypeCd() {
		return typeCd;
	}
	public void setTypeCd(Integer typeCd) {
		this.typeCd = typeCd;
	}
	public ZonedDateTime getCreateUTCTimestamp() {
		return createUTCTimestamp;
	}
	public void setCreateUTCTimestamp(ZonedDateTime createUTCTimestamp) {
		this.createUTCTimestamp = createUTCTimestamp;
	}
	public Integer getPriorityCd() {
		return priorityCd;
	}
	public void setPriorityCd(Integer priorityCd) {
		this.priorityCd = priorityCd;
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
		//Types of payload messages
		public static final int TYPE_CD_MOUSE_COORDINATES = 0;
		
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
}
