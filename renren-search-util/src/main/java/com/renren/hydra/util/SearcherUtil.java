package com.renren.hydra.util;

import Ice.Identity;

public class SearcherUtil {
	public static String createEndpoints(String ip, String port) {
		String endpoints = "tcp -h " + ip + " -p " + port;
		return endpoints;
	}
	
	public static Identity createIceIdentity(String business) {
		return Ice.Util.stringToIdentity(business);
	}
	
	public static String createAdapterName(String business) {
		return business;
	}
}
