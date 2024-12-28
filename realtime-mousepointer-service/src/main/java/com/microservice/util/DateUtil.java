package com.microservice.util;

import java.time.ZonedDateTime;

public class DateUtil {
//	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Get timezone offset of current JVM in minutes.
	 * @return
	 */
	public static Integer getSysTimezoneOffsetMins() {
		return ZonedDateTime.now().getOffset().getTotalSeconds()/60;
	}
	 
	 /**
	 * Get timezone offset of current JVM in minutes. (JS accepts +ve values of -ve UTC timezones, whereas in ZonedDateTime its opposite)
	 * @return
	 */
	public static Integer getSysTimezoneOffsetMinsJS() {
		return -1 * DateUtil.getSysTimezoneOffsetMins();
	}
}
