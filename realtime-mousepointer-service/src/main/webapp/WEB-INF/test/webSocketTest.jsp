<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>

<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC" crossorigin="anonymous">
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-MrcW6ZMFYlzcLA8Nl+NtUVF0sA7MsXsP1UyJoMp4YLEuNSfAP+JcXn/tWtIaxVXM" crossorigin="anonymous"></script>
<script src="${pageContext.request.contextPath}/js/jquery/jquery-3.7.1.min.js"></script>

<style>
	body {
         height: 100vh;
         margin: 0;
         overflow: hidden;
         position: relative;
     }
    .chat-container {
        max-width: 600px;
        margin: 20px auto;
        border: 1px solid #dee2e6;
        border-radius: 5px;
        overflow: hidden;
    }
    .chat-window {
        height: 400px;
        overflow-y: auto;
        padding: 10px;
        background: #f8f9fa;
    }
    .message {
        margin-bottom: 10px;
        padding: 10px;
        border-radius: 8px;
        max-width: 70%;
    }
    .my-message {
        background: #007bff;
        color: white;
        margin-left: auto;
    }
    .other-message {
        background: #e9ecef;
    }
    .timestamp {
        font-size: 0.8em;
        color: #6c757d;
        margin-top: 5px;
    }
    
    .cursor-indicator {
            position: absolute;
    pointer-events: none;
    transition: transform 0.1s ease-out;
        }

        .cursor-arrow {
            width: 0;
		    height: 0;
		    border-left: 6px solid transparent;
		    border-right: 6px solid transparent;
		    border-bottom: 12px solid;
		    transform: rotateZ(-45deg);
        }

        .cursor-name {
            padding: 5px 10px;
		    border-radius: 12px;
		    font-size: 12px;
		    color: #fff;
		    white-space: nowrap;
			margin-left: .5rem !important;
        }
</style>
</head>
<body>
${curHostAndPort}

<div class="chat-container">
    <div class="chat-window" id="chatWindow"></div>
    <div class="p-3 border-top">
        <div class="input-group">
            <input type="text" id="chatInput" class="form-control" placeholder="Type a message...">
            <button class="btn btn-primary" id="sendBtn">Send</button>
        </div>
    </div>
</div>

</body>
<script src="${pageContext.request.contextPath}/js/util/webSocketUtils.js"></script>
<script>

const subscriberId = (1+parseInt(Math.random()*100)) + "";
const MOUSE_UPDATES_PER_SECOND = 70;
const topicPrevPersistenceMap = new Map();
const userInfoMap = new Map();
let currentlyCatchingUp = false;

//the last time we were up-to-date on all our messages
let lastServerPongTime = null;

const socket = new WebSocket("ws://127.0.0.1${pageContext.request.contextPath}/websocket?userId="+subscriberId);
console.log(socket);
socket.onopen = function(event) {
    console.log("WebSocket connection established.");
};

socket.onmessage = function(event) {
    const messageData = JSON.parse(event.data);
    if(messageData.createSubscriberId == subscriberId){
		return;	//ignore messages created by myself. These messages may be useful in missed messages catchup
    }
    let payloadObj = null;
    if(messageData?.payload?.payloadValue){
    	payloadObj = JSON.parse(messageData.payload.payloadValue);
	}
    
    if(messageData.persistentMsgCd == 1){
		console.log(messageData);
    }
	switch(messageData.typeCd){
		case WebSocketMessage.TYPE_CD_PUBLISH:
			//check if persistent messages lost or not
			if(messageData.previousPersistenceId){
				let storedPrevPersistencedId = topicPrevPersistenceMap.get(messageData.targetTopicId);
				if(storedPrevPersistenceId != messageData.previousPersistenceId){
					//server in-midst of catching up lost messages. When catchUp complete, then we can start accepting messages again.
					if(currentlyCatchingUp){
						return;
					}
					//notify server to rewind message stream to our last seen message and start sending messages from there
					let catchupMsg = new WebSocketMessage();
					catchupMsg.typeCd = WebSocketMessage.TYPE_CD_CATCHUP_REQUEST;
					catchupMsg.createSubscriberId = subscriberId;
					catchupMsg.targetTopicId = messageData.targetTopicId;
					catchupMsg.prevousPersistenceId = storedPrevPersistencedId;
					WebSocketUtil.sendMessage(socket, catchupMsg);
					return;
				}
			}
		    switch(messageData.payload.typeCd){
		    	case WebSocketMessagePayload.TYPE_CD_MOUSE_COORDINATES:
		    		moveCursor(payloadObj.userId, payloadObj.x, payloadObj.y);
		    		
		    	//	$("#userPointer").css("left", payloadObj.x+"px");
		    		//$("#userPointer").css("top", payloadObj.y+"px");
		    		//console.log("Got coordinates for userId: "+payloadObj.userId+" x="+payloadObj.x+", y="+payloadObj.y);
		    	break;
		    	case WebSocketMessagePayload.TYPE_CD_CHAT_MESSAGE:
		    		let userId = payloadObj.userId;
		    		let userInfo = userInfoMap.get(userId);
		    		addMessage(userId, userInfo.username, payloadObj.chatMessage, messageData.createTimeUtcMs);
		    		break;
		    }
		    break;
		case WebSocketMessage.TYPE_CD_PONG:
			lastServerPongTime = messageData.createTimeUtcMs;
			break;
		case WebSocketMessage.TYPE_CD_CATCHUP_COMPLETE:
			currentlyCatchingUp = false;
			break;
		case WebSocketMessage.TYPE_CD_SUBSCRIBE:
			switch(messageData.payload.typeCd){
			case WebSocketMessagePayload.TYPE_CD_USER_CONNECTED:
				userInfoMap.set(payloadObj.userId, payloadObj);	//add user's info
				createCursor(payloadObj.userId, payloadObj.firstName)
				break;
			}
			break;
		case WebSocketMessage.TYPE_CD_UNSUBSCRIBE:
			switch(messageData.payload.typeCd){
				case WebSocketMessagePayload.TYPE_CD_USER_DISCONNECTED:
					userInfoMap.delete(payloadObj.userId);	//remove user's info
					removeCursor(payloadObj.userId);
					break;
			}
			break;
			
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
	let subscriptTopicMsg = new WebSocketMessage();
	subscriptTopicMsg.typeCd = WebSocketMessage.TYPE_CD_SUBSCRIBE;
	subscriptTopicMsg.createSubscriberId = subscriberId;
	subscriptTopicMsg.targetTopicId = '1';
	subscriptTopicMsg.persistentMsgCd = 1;
	
	let msgPayload = new WebSocketMessagePayload();
	msgPayload.typeCd = WebSocketMessagePayload.TYPE_CD_USER_CONNECTED;
	msgPayload.payloadValue = JSON.stringify({ userId: subscriberId, username: 'ArmaanAdmin', firstName: 'Armaan', lastName: 'Name'});
	subscriptTopicMsg.payload = msgPayload;
	WebSocketUtil.sendMessage(socket, subscriptTopicMsg);
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
	mouseCoordMsg.targetTopicId = '1';
	
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
  Chat window
 */
 
 function addMessage(userId, username, message, timestamp = Date.now()) {
     const chatWindow = $('#chatWindow');
     const time = new Date(timestamp).toLocaleTimeString();
     const isMine = userId === subscriberId;
     const alignment = isMine ? 'my-message' : 'other-message';

     // Create the message HTML
     const messageHtml = `
         <div class="message \${alignment}" data-timestamp="\${timestamp}">
             <strong>\${username}</strong>
             <div>\${message}</div>
             <div class="timestamp">\${time}</div>
         </div>`;
	
     let added = false;
     $('.message').each(function () {
         const existingTimestamp = parseInt($(this).data('timestamp'));
         if (timestamp < existingTimestamp) {
             $(this).before(messageHtml);
             added = true;
             return false;
         }
     });

     if (!added) {
         chatWindow.append(messageHtml);
     }

     chatWindow.scrollTop(chatWindow[0].scrollHeight);
 }

 $('#sendBtn').click(() => {
	const message = $('#chatInput').val();
	    
	let chatMsg = new WebSocketMessage();
	chatMsg.typeCd = WebSocketMessage.TYPE_CD_PUBLISH;
	chatMsg.createSubscriberId = subscriberId;
	chatMsg.targetTopicId = '1';
	chatMsg.persistentMsgCd = 1;
	
	let msgPayload = new WebSocketMessagePayload();
	msgPayload.typeCd = WebSocketMessagePayload.TYPE_CD_CHAT_MESSAGE;
	msgPayload.payloadValue = JSON.stringify({ userId: subscriberId, chatMessage: message});
	chatMsg.payload = msgPayload;
	WebSocketUtil.sendMessage(socket, chatMsg);
		
    if (message.trim()) {
    	addMessage(subscriberId, 'Me', message);
    	$('#chatInput').val('');
	}
 });
 
 /*
  Realtime Cursor code
 */
 const usedColors = new Set();
 const cursorMap = new Map();

 function generateBrightColor() {
     let r, g, b;
     do {
         r = Math.floor(Math.random() * 156) + 100; // Bright R (100-255)
         g = Math.floor(Math.random() * 156) + 100; // Bright G (100-255)
         b = Math.floor(Math.random() * 156) + 100; // Bright B (100-255)
     } while (usedColors.has(`rgb(\${r},\${g},\${b})`));
     return `rgb(\${r},\${g},\${b})`;
 }

 function getUniqueColor() {
     const color = generateBrightColor();
     usedColors.add(color);
     return color;
 }

 function releaseColor(color) {
     usedColors.delete(color);
 }

 function createCursor(userId, name) {
	 if (cursorMap.has(userId)) return;

     const color = getUniqueColor();
     const cursor = $(`<div class="cursor-indicator" id="cursor-\${userId}">
         <div class="cursor-arrow" style="border-bottom-color: \${color}"></div>
         <div class="cursor-name" style="background-color: \${color}">\${name}</div>
     </div>`);

     $('body').append(cursor);
     cursorMap.set(userId, { element: cursor, color });
 }

 function moveCursor(userId, x, y) {
     const cursor = cursorMap.get(userId)?.element;
     if (cursor) {
         cursor.css({ left: `\${x}px`, top: `\${y}px` });
     }
 }

 function removeCursor(userId) {
     const cursorInfo = cursorMap.get(userId);
     if (cursorInfo) {
         cursorInfo.element.remove();
         releaseColor(cursorInfo.color);
         cursorMap.delete(userId);
     }
 }
</script>
</html>