package com.microservice.util;

import java.lang.reflect.Type;
import java.time.LocalDateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class JsonUtil {
	private static Gson gson;
	static {
		gson = new GsonBuilder()
				//Adapters for LocalDateTime
				.registerTypeAdapter(LocalDateTime.class, new JsonSerializer<LocalDateTime>() {
					@Override
					public JsonElement serialize(LocalDateTime src, Type typeOfSrc, JsonSerializationContext context) {
						if(src==null) {
							return null;
						}
						return new JsonPrimitive(src.toString());
					}
				})
				.registerTypeAdapter(LocalDateTime.class, new JsonDeserializer<LocalDateTime>() {

					@Override
					public LocalDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
							throws JsonParseException {
						if(json == null || json.getAsString() == null || json.isJsonNull()) {
							return null;
						}
						return LocalDateTime.parse(json.getAsString());
					}
				})
				
//				//Adapters for ZonedDateTime
//				.registerTypeAdapter(ZonedDateTime.class, new JsonSerializer<ZonedDateTime>() {
//					@Override
//					public JsonElement serialize(ZonedDateTime src, Type typeOfSrc, JsonSerializationContext context) {
//						if(src==null) {
//							return null;
//						}
//						return new JsonPrimitive(src.format(DateUtil.defaultClientDateFormatter));
//					}
//				})
//				.registerTypeAdapter(ZonedDateTime.class, new JsonDeserializer<ZonedDateTime>() {
//
//					@Override
//					public ZonedDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
//							throws JsonParseException {
//						if(json == null || json.getAsString() == null || json.isJsonNull()) {
//							return null;
//						}
//						return ZonedDateTime.parse(json.getAsString());
//					}
//				})
				.create();
	}
	
	/**
	 * Convert given object to JSON representation
	 * @param src
	 * @return
	 */
	public static String toJson(Object src) {
		return gson.toJson(src);
	}
	
	/**
	 * Convert JSON to given Object of type 'type'
	 * @param <T>
	 * @param json
	 * @param type
	 * @return
	 */
	public static <T> T fromJson(String json, Type type) {
		return gson.fromJson(json, type);
	}
}
