//Match with com.microservice.pubsub.InMemoryWebSocketMessage
class WebSocketMessage{
	static TYPE_CD_PUBLISH = 0;
	static TYPE_CD_SUBSCRIBE = 1;
	static TYPE_CD_PING = 100;
	static TYPE_CD_PONG = 101;
		
	static PRIORITY_CD_HIGH = 1;
	static PRIORITY_CD_LOW = 0;
	
	typeCd;
	payloadTypeCd;
	priorityCd;
	payload;	//typeWebSocketMessagePayload
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

class WebSocketMessagePayload {
	static TYPE_CD_MOUSE_COORDINATES = 0;
	
	typeCd;
	payloadValue;
}
function setupWSPingPongLoop(){
	//loop
	console.log("Setting up Ping Pong loop");
}