<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
<style>
.mouse-pointer{
	height: 15px;
    width: 15px;
    background: black;
    position: absolute;
    border-radius: 100%;
}
</style>

<script src="${pageContext.request.contextPath}/js/jquery/jquery-3.7.1.min.js"></script>
</head>
<body>
<div id="userPointer" class="mouse-pointer"></div> 
${curHostAndPort}
</body>
<script src="${pageContext.request.contextPath}/js/util/webSocketUtils.js"></script>
<script>

const subscriberId = 1+parseInt(Math.random()*100);
const MOUSE_UPDATES_PER_SECOND = 70;

const socket = new WebSocket("ws://127.0.0.1${pageContext.request.contextPath}/websocket?userId="+subscriberId);
console.log(socket);
socket.onopen = function(event) {
    console.log("WebSocket connection established.");
};

socket.onmessage = function(event) {
    const messageData = JSON.parse(event.data);
    console.log(messageData);
    if(messageData.typeCd == WebSocketMessage.TYPE_CD_PUBLISH){
	    switch(messageData.payload.typeCd){
	    	case WebSocketMessagePayload.TYPE_CD_MOUSE_COORDINATES:
	    		const payloadObj = JSON.parse(messageData.payload.payloadValue);
	    		$("#userPointer").css("left", payloadObj.x+"px");
	    		$("#userPointer").css("top", payloadObj.y+"px");
	    		//console.log("Got coordinates for userId: "+payloadObj.userId+" x="+payloadObj.x+", y="+payloadObj.y);
	    	break;
	    	default:
	    }
    }
    
    // Handle incoming message data
    //console.log("Received message:", messageData);
};

socket.onerror = function(error) {
    console.error("WebSocket error: ", error);
};

socket.onclose = function(event) {
    console.log("WebSocket connection closed:", event);
};

socket.onopen = (event) => {
	//setup ping pong loop with server. For socket maintenance purposes
	WebSocketUtil.setupPingPongLoop(socket);
	
	//register subscribe to classRoom1
	let subTopic = new WebSocketMessage();
	subTopic.typeCd = WebSocketMessage.TYPE_CD_SUBSCRIBE;
	subTopic.createSubscriberId = subscriberId;
	subTopic.subscribeTopicId = 1;
	WebSocketUtil.sendMessage(socket, subTopic);
};

//Need to throttle mouse-pointers updates to a good amount
function throttle(func, timesPerSecond) {
  const limit = 1000 / timesPerSecond; // Calculate the interval in milliseconds
  let lastCall = 0;
  
  return function(...args) {
    const now = Date.now();
    if (now - lastCall >= limit) {
      lastCall = now;
      func(...args);
    }
  };
}
 

//Function to capture and send mouse coordinates
function sendMouseCoordinates(event) {  
	let mouseCoordMsg = new WebSocketMessage();
	mouseCoordMsg.typeCd = WebSocketMessage.TYPE_CD_PUBLISH;
	mouseCoordMsg.createSubscriberId = subscriberId;
	mouseCoordMsg.publishTopicId = 1;
	
	//actual coordinates
	let x = event.clientX;
	let y = event.clientY;
	let msgPayload = new WebSocketMessagePayload();
	msgPayload.typeCd = WebSocketMessagePayload.TYPE_CD_MOUSE_COORDINATES;
	msgPayload.payloadValue = JSON.stringify({ x: x, y: y, userId: subscriberId});
	
	mouseCoordMsg.payload = msgPayload;
	WebSocketUtil.sendMessage(socket, mouseCoordMsg);
}

// Throttle the mousemove event to MOUSE_UPDATES_PER_SECOND times per second
const throttledMouseMove = throttle(sendMouseCoordinates, MOUSE_UPDATES_PER_SECOND);

// Add mousemove event listener
document.addEventListener("mousemove", throttledMouseMove);

/*
 
 */

</script>
</html>