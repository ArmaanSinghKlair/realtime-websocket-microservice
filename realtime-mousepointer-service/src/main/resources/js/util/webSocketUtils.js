//Match with com.microservice.pubsub.InMemoryWebSocketMessage
class WebSocketMessage{
	static TYPE_CD_PUBLISH = 0;
	static TYPE_CD_SUBSCRIBE = 1;
	static TYPE_CD_PING = 100;
	static TYPE_CD_PONG = 101;
	
	static PING_PONG_INTERVAL = 10 *1000;	//10 seconds
	
	static PERSISTENT_MESSAGING_CD_YES = 1;
	static PERSISTENT_MESSAGING_CD_NO = 0;

	typeCd;
	payloadTypeCd;
	payload;	//typeWebSocketMessagePayload
	createTimeUtcMs;
	timezoneOffsetMins;	//reference. In-case message origin timezone is needed
	createSubscriberId;
	
	//Used when subscribing to topic
	subscribeTopicId;
	subscribeTopicPersistentMessagingCd;
	
	//Used when publishing messages to topic
	publishTopicId;
	
	constructor(){
		this.createTimeUtcMs = Date.now();
		this.timezoneOffsetMins = new Date().getTimezoneOffset() 
	}
}

class WebSocketMessagePayload {
	typeCd;
	payloadValue;
}

class WebSocketUtil {
	/**
	 * Util class for setting up ping-pong loop
	 */
	static setupPingPongLoop(socket, successFn, failureFn){
		console.log("Setting up Ping Pong loop");

		const heartbeatInterval = setInterval(() => {
			let pingMsg = new WebSocketMessage();
			pingMsg.typeCd = WebSocketMessage.TYPE_CD_PING;
			let heartbeatFailureFn = (error) =>{
				clearInterval(heartbeatInterval); // Stop the interval if the connection is closed
				console.log("Got error in ping-pong. Clearing current ping-pong and retrying ping-pong connection");
				alert("Failure talking to servers. Please check your connection or try reloading the page.");
				if(failureFn){
					failureFn();
				}
			}
			WebSocketUtil.sendMessage(socket, pingMsg, successFn, heartbeatFailureFn);
	    }, WebSocketMessage.PING_PONG_INTERVAL); // 30 seconds interval		
	}
	
	/**
	 * Util class for sending websocket messages
	 */
	static sendMessage(socket, payload, successFn, failureFn) {
	    if (socket.readyState === WebSocket.OPEN) {
			try{
	        	socket.send(JSON.stringify(payload));
				if(successFn){
					successFn();
				}
			} catch (error){
				console.log("Error while sending message:", error);
				if(failureFn){
					failureFn(error);
				}
			}
	    } else {
	        console.error("WebSocket connection is not open.");
			if(failureFn){
				failureFn("WebSocket connection is not open.");
			}
	    }
	}
} 


