//Match with com.microservice.pubsub.InMemoryWebSocketMessage
class WebSocketMessage{
	static TYPE_CD_PUBLISH = 0;
	static TYPE_CD_SUBSCRIBE = 1;
	static TYPE_CD_PING = 100;
	static TYPE_CD_PONG = 101;
		
	static PRIORITY_CD_HIGH = 1;
	static PRIORITY_CD_LOW = 0;
	
	static PING_PONG_INTERVAL = 30 *1000;	//30 seconds
	typeCd;
	payloadTypeCd;
	priorityCd;
	payload;	//typeWebSocketMessagePayload
	createTimeUtcMs;
	timezoneOffsetMins;	//reference. In-case message origin timezone is needed
	createSubscriberId;
	subscribeTopicId;
	publishTopicId;
	
	constructor(){
		this.priorityCd = this.PRIORITY_CD_LOW;
		this.createTimeUtcMs = Date.now();
		this.timezoneOffsetMins = new Date().getTimezoneOffset() 
	}
}

class WebSocketMessagePayload {
	static TYPE_CD_MOUSE_COORDINATES = 0;
	
	typeCd;
	payloadValue;
}

class WebSocketUtil {
	/**
	 * Util class for setting up ping-pong loop
	 */
	static setupPingPongLoop(socket, successFn, failureFn){
		const heartbeatInterval = setInterval(() => {
			console.log("Setting up Ping Pong loop");
			let pingMsg = new WebSocketMessage();
			pingMsg.typeCd = WebSocketMessage.TYPE_CD_PING;
			let heartbeatFailureFn = (error) =>{
				clearInterval(heartbeatInterval); // Stop the interval if the connection is closed
				console.log("Got error in ping-pong. Clearing current ping-pong and retrying ping-pong connection");
				alert("Failure talking to servers. Please check your connection.");
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


