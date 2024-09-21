package com.microservice.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {
	private static final Logger logger = LoggerFactory.getLogger(DateUtil.class);

	/**
	 * All timestamps from clients are stored in ISO 8601 with Zulu offset (ie UTC timezone) eg. 2024-09-21T21:02:48.252Z
	 */
	public static final DateTimeFormatter defaultClientDateFormatter =  DateTimeFormatter.ISO_INSTANT;
	
	/**
	 * Convert ZonedDateTime to current timezone localDateTime
	 * @param zonedDateTime
	 * @return
	 */
	public static LocalDateTime getSystemDateTimeFromZoned(ZonedDateTime zonedDateTime) {
		if(zonedDateTime == null) {
			return null;
		}
		return zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
	}
	
	/**
	 * Convert zoned timestamp to current system timezone
	 * @param isoDateString
	 * @return
	 */
	public static LocalDateTime getSystemDateTimeFromISO(String isoDateString) {
		try {
			ZonedDateTime zoneTime = ZonedDateTime.parse(isoDateString, DateTimeFormatter.ISO_DATE_TIME);
		    // convert ZonedDateTime to LocalDateTime in local time zone
			return zoneTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
		} catch(Exception e) {
			logger.error("Error getLocalFromZonedDateTime", e);
			return null;
		}
	}
	
	/**
	 * Convert zoned timestamp to current system timezone
	 * @param isoDateString
	 * @param targetOffsetMins Specifies how far is UTC timezone from target timezone eg. 360 = UTC is 360 mins ahead. Therefore, to convert UTC to target we have to -1*offsetMins
	 * @return
	 */
	public static LocalDateTime getClientDateTimeFromISO(String isoDateString, Integer targetOffsetMins) {
		try {
			ZonedDateTime zoneTime = ZonedDateTime.parse(isoDateString, DateTimeFormatter.ISO_DATE_TIME);
		    // convert ZonedDateTime to LocalDateTime in local time zone
			return zoneTime.withZoneSameInstant(ZoneOffset.ofTotalSeconds(-1 * targetOffsetMins * 60)).toLocalDateTime();
		} catch(Exception e) {
			logger.error("Error getClientDateTimeFromISO", e);
			return null;
		}
	}
}
