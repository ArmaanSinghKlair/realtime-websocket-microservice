package com.microservice.util;

import java.util.Map;

import com.microservice.contract.CacheExpireStrategy;

/**
 * Builder pattern. This is for future-proofing CacheCleanupConfig where different params need to be set for different cache strategies.
 * Also the same series of steps can be reused instead
 */
public class CacheCleanupConfigBuilder<K,V> {
	private Map<K, CacheEntry<V>> cache;
	private Integer expireMins;
	private CacheExpireStrategy<K,V> expireStrategy;
	

	public Map<K, CacheEntry<V>> getCache() {
		return cache;
	}

	public Integer getExpireMins() {
		return expireMins;
	}

	public CacheExpireStrategy<K, V> getExpireStrategy() {
		return expireStrategy;
	}

	public CacheCleanupConfigBuilder<K,V> setCache(Map<K, CacheEntry<V>> cache) {
		this.cache = cache;
		return this;
	}

	public CacheCleanupConfigBuilder<K,V> setExpireMins(Integer expireMins) {
		this.expireMins = expireMins;
		return this;
	}

	public CacheCleanupConfigBuilder<K,V> setExpireStrategy(CacheExpireStrategy<K, V> expireStrategy) {
		this.expireStrategy = expireStrategy;
		return this;
	}
	
	public CacheCleanupConfig<K,V> build(){
		return new CacheCleanupConfig<K,V>(this);
	}
}
