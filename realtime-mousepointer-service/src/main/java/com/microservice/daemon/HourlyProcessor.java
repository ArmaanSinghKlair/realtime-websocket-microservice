package com.microservice.daemon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.microservice.pubsub.WebSocketPubSubBroker;
import com.microservice.util.CacheCleanupConfig;
import com.microservice.util.CacheEntry;

@Component
public class HourlyProcessor {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	/**
	 * List of each <cache, cacheCleanupConfig> for all temp caches in the system
	 */
	public static List<CacheCleanupConfig<?,?>> hourlyCacheCleanupList = new ArrayList<>();
	/**
	 * Any hourly cleanup is done here
	 */
	void hourlyCacheCleanup() {
		try {
			//Clear expired cache entries
			for(CacheCleanupConfig<?, ?> cleanupConfig: hourlyCacheCleanupList) {
				cleanupConfig.clearExpiredCache();
			}
		} catch(Exception e) {
			logger.error("Error in hourlyCacheCleanup", e);
		}
		
	}
}
