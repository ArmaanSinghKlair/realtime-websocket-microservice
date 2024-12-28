package com.microservice.daemon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.microservice.pubsub.InMemoryWebSocketPubSubBroker;
import com.microservice.util.CacheCleanupConfig;
import com.microservice.util.CacheEntry;

@Component
public class HourlyProcessor {
	/**
	 * Deduplication at pub-sub broker level
	 * Rolling cache. All entries before 5 mins can be cleared safely.
	 * Rolling cache avoids race conditions with cleanup daemon and recently created topics
	 * 
	 */
	//Biolerplate code to register caches to be cleanedup here
	//register this cache to be cleaned up hourly
//	static CacheCleanupConfigBuilder<Long,Boolean> ttlCacheConfigBuilder = new CacheCleanupConfigBuilder<Long,Boolean>();
//	static {
//		ttlCacheConfigBuilder.setExpireMins(5); //entries older than 5 mins should be cleaned up
//		ttlCacheConfigBuilder.setExpireStrategy(new TTLCacheExpireStrategy<Long, Boolean>());
//	}
	//Map<Long, CacheEntry<Boolean>> 
	//topicLockMap.computeIfAbsent(topic.getTopicId(), k -> new CacheEntry<Boolean>(true).setCreatedTimestamp(LocalDateTime.now()))
	
//	static {
//		//register this cache 
//    	ttlCacheConfigBuilder.setCache(topicLockMap);
//    	HourlyProcessor.hourlyCacheCleanupList.add(ttlCacheConfigBuilder.build());
//    }
	
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
