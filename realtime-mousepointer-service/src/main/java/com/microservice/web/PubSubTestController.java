package com.microservice.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

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
	@RequestMapping(method=RequestMethod.GET, path = "/webSocketTest.html")
	public ModelAndView getPubSubSubscriberInfoGET(HttpServletRequest request, HttpServletResponse response){
		try {
			return new ModelAndView("test/webSocketTest");
		} catch(Exception e) {
			logger.error("Error in getPubSubSubscriberInfoGET controller", e);
			response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
			return new ModelAndView("general/genericError", "message", e.getMessage());
		}
	}
}
