package com.renren.hydra.util;

public class TimeUtil {
	public static String getTime(String time) {
		if (time.endsWith(".0")) {
			return time.substring(0, time.length() - 2).replaceAll("-|:| ", "");
		} else {
			return time.replaceAll("-|:| ", "");
		}
	}
}
