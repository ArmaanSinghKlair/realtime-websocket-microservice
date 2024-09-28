package com.microservice.web;

import java.time.LocalDateTime;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.microservice.pubsub.InMemoryWebSocketMessage;
import com.microservice.pubsub.InMemoryWebSocketSubscriber;
import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketPubSubBroker;
import com.microservice.websocket.WebSocketMessageHandler;

import jakarta.servlet.http.HttpServletRequest;

@Controller
public class PubSubInfoController {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Returns queued messages for each subscriber across the broker
	 * @param request
	 * @return
	 */
	@RequestMapping(method=RequestMethod.GET, path = "/getPubSubSubscriberInfo.html")
	public ResponseEntity<String> getPubSubSubscriberInfoGET(HttpServletRequest request){
		try {
			StringBuilder response = new StringBuilder();
			HashSet<Long> subscriberIdSet = new HashSet<>();
			for(InMemoryWebSocketTopic topic: WebSocketPubSubBroker.topicMap.values()) {
				subscriberIdSet.addAll(topic.getSubscriberSet());
			}
			for(Long subscriberId: subscriberIdSet) {
				response.append("<p>Subscriber #"+subscriberId+":<br>");
				//TEST
				LocalDateTime testLastMessage = null;
				Long totMsgsConsumed = 0l;
				int conNum=1;
				for(InMemoryWebSocketSubscriber subscriber: WebSocketMessageHandler.connectionBySubscriberMap.get(subscriberId)) {
					//TEST
					testLastMessage = subscriber.lastTestTime;
					totMsgsConsumed += subscriber.getTopicQueue().size();
					
					response.append("<p style='margin-left:20px'>Connection #"+conNum+"<br>");
					int msgNum=1;
//					for(InMemoryWebSocketMessage message: subscriber.getTopicQueue()) {
//						response.append("<p style='margin-left:30px'>Queued Msg #"+msgNum+": <i><b>"+message.getPayload()+"</b></i></p>");
//						msgNum++;
//					}
					response.append("</p>");
					conNum++;
				}
				//TEST
				response.append("Last message consumed time: "+testLastMessage.toString()+", Total messages consumed = "+totMsgsConsumed);
				response.append("</p>");
			}
			return new ResponseEntity<>(response.toString(), HttpStatus.OK);
		} catch(Exception e) {
			logger.error("Error in getPubSubSubscriberInfoGET controller", e);
			return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	/**
	 * Returns queued messages for each topic across the broker
	 * @param request
	 * @return
	 */
	@RequestMapping(method=RequestMethod.GET, path = "/getPubSubTopicInfo.html")
	public ResponseEntity<String> getPubSubTopicInfo(HttpServletRequest request){
		try {
			StringBuilder response = new StringBuilder();
			for(InMemoryWebSocketTopic topic: WebSocketPubSubBroker.topicMap.values()) {
				response.append("<p>Topic #"+topic.getTopicId()+" ("+topic.getName()+"):<br>");
				int msgNum=1;
				for(InMemoryWebSocketMessage message: topic.getTopicQueue()) {
					response.append("<p style='margin-left:30px'>Queued Msg #"+msgNum+": <i><b>"+message.getPayload()+"</b></i></p>");
					msgNum++;
				}
				response.append("</p>");
			}
			
			return new ResponseEntity<>(response.toString(), HttpStatus.OK);
		} catch(Exception e) {
			logger.error("Error in getPubSubSubscriberInfoGET controller", e);
			return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
}
