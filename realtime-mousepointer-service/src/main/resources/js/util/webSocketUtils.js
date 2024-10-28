//Match with com.microservice.pubsub.InMemoryWebSocketMessage
class WebSocketMessage{
	static TYPE_CD_PUBLISH = 0;
	static TYPE_CD_SUBSCRIBE = 1;
	static TYPE_CD_PING = 100;
	static TYPE_CD_PONG = 101;
	
	static PRIORITY_CD_HIGH = 1;
	static PRIORITY_CD_LOW = 0;
	
	typeCd;
	priorityCd;
	payload;
	createUTCTimestamp;
	timezoneOffsetMins;
	createSubscriberId;
	subscribeTopicId;
	publishTopicId;
	
	constructor(){
		this.priorityCd = this.PRIORITY_CD_LOW;
		this.createUTCTimestamp = new Date().toISOString();
		this.timezoneOffsetMins = new Date().getTimezoneOffset() 
	}
}

function setupWSPingPongLoop(){
	//loop
}