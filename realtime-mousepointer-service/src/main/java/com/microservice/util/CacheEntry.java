package com.microservice.util;

import java.time.LocalDateTime;

/**
 * Utility class for storing cache entries with timestamp if needed.
 * Builder pattern followed here in all setters.
 * @param <E>
 */
public class CacheEntry<E> {
	private LocalDateTime createdTimestamp;
	private E data;
	
	public CacheEntry(E data) {
		this.data = data;
	}
	public LocalDateTime getCreatedTimestamp() {
		return createdTimestamp;
	}
	public CacheEntry<E> setCreatedTimestamp(LocalDateTime createdTimestamp) {
		this.createdTimestamp = createdTimestamp;
		return this;
	}
	public E getData() {
		return data;
	}
	public CacheEntry<E> setData(E data) {
		this.data = data;
		return this;
	}
	
}
