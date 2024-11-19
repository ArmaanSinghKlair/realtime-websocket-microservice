package com.microservice.web.test;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import com.microservice.pubsub.InMemoryWebSocketTopic;
import com.microservice.pubsub.WebSocketPubSubBroker;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Controller
public class PubSubTestController {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Returns webSocketTest jsp
	 * @param request
	 * @return
	 */
	@GetMapping("/webSocketTest.html")
	public ModelAndView getPubSubSubscriberInfoGET(HttpServletRequest request, HttpServletResponse response){
		try {
			return new ModelAndView("test/webSocketTest");
		} catch(Exception e) {
			logger.error("Error in getPubSubSubscriberInfoGET controller", e);
			response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
			return new ModelAndView("general/genericError", "message", e.getMessage());
		}
	}
	
	@GetMapping("/getPubSubTopicInfo.html")
	@ResponseBody
	public String getPubSubTopicInfoGET(HttpServletRequest request, HttpServletResponse response) {
		try {
			StringBuilder resultSb = new StringBuilder("""
					<h1>Topic Information</h1>
					<hr />
					""");
			for(Map.Entry<Long, InMemoryWebSocketTopic> entry: WebSocketPubSubBroker.topicMap.entrySet()) {
				int subscriberNum = entry.getValue().getSubscriberSet().size();
				resultSb.append("Topic ID: "+ entry.getKey()+"<br>");
				resultSb.append("Subscribers Num: "+subscriberNum+"<br>");
//				if(subscriberNum > 0) {
//					resultSb.append("Subscriber Ids: &nbsp;");
//					for(Long subscriberId: entry.getValue().getSubscriberSet()) {
//						resultSb.append(subscriberId+",");
//					}
//				}
				resultSb.append("<br>Queue Num: "+entry.getValue().getTopicQueue().size()+"<br>");
				
				resultSb.append("<br><br>");
			}
			return resultSb.toString();
		} catch(Exception e) {
			logger.error("Error in getPubSubTopicInfoGET controller", e);
			response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
			return e.getMessage();
		}
	}
}
