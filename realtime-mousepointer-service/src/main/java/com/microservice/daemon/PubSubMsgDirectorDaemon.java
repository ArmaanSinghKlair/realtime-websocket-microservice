package com.microservice.daemon;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.microservice.pubsub.InMemoryWebSocketMessage;

/**
 * Daemon class used in topics to process distribute messages to consumers
 */
@Component
public class PubSubMsgDirectorDaemon {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	AtomicBoolean isProcessingLock = new AtomicBoolean(false);
	
	
	private void execute() {
		
	}
}
