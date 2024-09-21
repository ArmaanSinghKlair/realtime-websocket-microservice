package com.microservice.util;

import java.util.Map;

import com.microservice.contract.CacheExpireStrategy;

/**
 * Store data related to cleanup of in-memory cache via various strategies (ie. {@linkplain CacheExpireStrategy})
 * Also contains routines that do the actual cleanup which can be called at program's convenience.
 */
public class CacheCleanupConfig<K,V> {
	private Map<K, CacheEntry<V>> cache;
	
	//Properties that help in determining if cacheEntries are expired
	private Integer expireMins;
	
	//Strategy that help determine expired cache entries for specified map
	private CacheExpireStrategy<K,V> expireStrategy;
	
	public CacheCleanupConfig(CacheCleanupConfigBuilder<K,V> builder) {
		this.cache = builder.getCache();
		this.expireMins = builder.getExpireMins();
		this.expireStrategy = builder.getExpireStrategy();
	}
	
	public Integer getExpireMins() {
		return expireMins;
	}
	public CacheExpireStrategy<K,V> getExpireStrategy() {
		return expireStrategy;
	}
	public void setExpireStrategy(CacheExpireStrategy<K,V> expireStrategy) {
		this.expireStrategy = expireStrategy;
	}
	public void setExpireMins(Integer expireMins) {
		this.expireMins = expireMins;
	}
	public Map<K, CacheEntry<V>> getCache() {
		return cache;
	}
	public void setCache(Map<K, CacheEntry<V>> cache) {
		this.cache = cache;
	}
	
	/**
	 * Deletes expired entries from cache.
	 * Can be called at your convenience
	 */
	public void clearExpiredCache() {
		if(cache == null) {
			throw new RuntimeException("Trying to clear NULL cache");
		}
		cache.entrySet().removeIf(entry -> this.expireStrategy.isCacheExpired(entry.getValue(), this));
	}
}
