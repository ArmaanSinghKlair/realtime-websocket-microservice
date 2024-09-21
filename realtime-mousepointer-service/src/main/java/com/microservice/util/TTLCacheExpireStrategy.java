package com.microservice.util;

import java.time.LocalDateTime;

import com.microservice.contract.CacheExpireStrategy;

/**
 * Expire cache entries older than certain time in past.
 */
public class TTLCacheExpireStrategy<K,V> implements CacheExpireStrategy<K,V>{

	@Override
	public <E> boolean isCacheExpired(CacheEntry<E> entry, CacheCleanupConfig<K,V> config) {
		return entry.getCreatedTimestamp().isBefore(LocalDateTime.now().minusMinutes((long)config.getExpireMins()));
	}

}
