package com.microservice.contract;

import com.microservice.util.CacheCleanupConfig;
import com.microservice.util.CacheEntry;

/**
 * When cleaning temporary caches, there can be multiple
 */
public interface CacheExpireStrategy<K,V> {
	/**
	 * Check if cache entry is expired or not
	 * @param entry	CacheEntry that needs to be tested.
	 * @param config Additional data that helps in making expired decision
	 * @return
	 */
	<E> boolean isCacheExpired(CacheEntry<E> entry, CacheCleanupConfig<K,V> config);
}
