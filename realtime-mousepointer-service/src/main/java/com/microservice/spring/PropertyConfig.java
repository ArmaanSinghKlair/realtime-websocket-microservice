package com.microservice.spring;

import org.springframework.context.annotation.Configuration;

@Configuration
public class PropertyConfig {
	public static final String PROPERTY_KEY_ENV_NAME = "environment.name";
	public static final String PROPERTY_KEY_RELEASE_VERSION = "release.version";
	public static final String PROPERTY_KEY_REDIS_HOST = "spring.data.redis.host";
	public static final String PROPERTY_KEY_REDIS_PORT = "spring.data.redis.port";
	public static final String PROPERTY_KEY_REDIS_USERNAME = "spring.data.redis.username";
	public static final String PROPERTY_KEY_REDIS_PASSWORD = "spring.data.redis.password";
	
	/**
	 * Container class for all properties (KEY, VALUES)
	 */
	public class Properties {
		public static enum EnvironmentName {
			DEV("dev"), STAGING("staging"), PRODUCTION("prod");
			
			private String value;
			private EnvironmentName(String value) {
				this.value = value;
			}
			public String getValue() {
				return value;
			}
		};
	}
}
