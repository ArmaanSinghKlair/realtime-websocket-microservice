package com.microservice.web.test;

import java.time.ZonedDateTime;

public class Test {
	public static void main(String[] args) {
		System.out.println(-1 * ZonedDateTime.now().getOffset().getTotalSeconds()/60);
	}
}
