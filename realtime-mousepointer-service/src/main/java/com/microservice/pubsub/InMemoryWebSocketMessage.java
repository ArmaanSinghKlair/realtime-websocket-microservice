package com.microservice.pubsub;

import java.time.ZonedDateTime;

public class InMemoryWebSocketMessage {
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
	private Integer priorityCd = PRIORITY_CD_LOW;	//whether messages needs to be durable OR NOT
	private String payload;
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
	public String getPayload() {
		return payload;
	}
	public void setPayload(String payload) {
		this.payload = payload;
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
	
}
