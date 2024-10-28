<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
ss
</body>
<script src="${pageContext.request.contextPath}/js/util/webSocketUtils.js"></script>
<script>
/*
const socket = new WebSocket("ws://localhost:8080/websocket");

socket.onopen = function(event) {
    console.log("WebSocket connection established.");
};

socket.onmessage = function(event) {
    const messageData = JSON.parse(event.data);
    // Handle incoming message data
    console.log("Received message:", messageData);
};

socket.onerror = function(error) {
    console.error("WebSocket error: ", error);
};

socket.onclose = function(event) {
    console.log("WebSocket connection closed:", event);
};

function sendMessage(message) {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(message));
    } else {
        console.error("WebSocket connection is not open.");
    }
}
exampleSocket.onopen = (event) => {
	//setup ping pong loop with server. For socket maintenance purposes
	setupWSPingPongLoop();
	
	//register subscribe to classRoom1
	let subTopic = new WebSocketMessage();
	subTopic.typeCd = WebSocketMessage.TYPE_CD_SUBSCRIBE;
	subTopic.createSubscriberId = 1+parseInt(Math.random()*100);
	subTopic.subscribeTopicId = 1;
	sendMessage(subTopic);
};
*/

</script>
</html>