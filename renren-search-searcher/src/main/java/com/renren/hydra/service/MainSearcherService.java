package com.renren.hydra.service;

import java.util.Date;

import org.apache.log4j.*;

import xce.util.tools.IPAddress;
import Ice.Communicator;
import Ice.Properties;
import IceBox.Service;

import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.util.SearcherUtil;
import com.renren.hydra.searcher.SearcherI;

/**
 * Ice服务,启动Searcher
 */
public class MainSearcherService implements Service {
	private Ice.ObjectAdapter _adapter;
	private static Logger logger = Logger.getLogger(MainSearcherService.class);
	private SearcherI _searcher;

	@Override
	public void start(String name, Communicator ic, String[] args) {
		Date date = new Date();
		Properties p = ic.getProperties();
		String confDir = p.getPropertyWithDefault("Config",
				"../etc/classes/node");

		String business = p.getProperty("BusinessName");
		HydraConfig config = HydraConfig.getInstance();
		if (!config.init(confDir)) {
			logger.error("fail to start SearcherService " + date.toString());
			return;
		}
		logger.info("start SearcherService   " + date.toString());
		String ip = IPAddress.getLocalAddress();
		String port = p.getPropertyWithDefault("Port", "10028");
		String endpoints = SearcherUtil.createEndpoints(ip, port);

		_adapter = ic.createObjectAdapterWithEndpoints(name, endpoints);
		_searcher = new SearcherI(config, business, ip, port);
		if (!_searcher.init()) {
			logger.error("fail to init SearcherI.");
			return;
		}

		_adapter.add(_searcher, ic.stringToIdentity(business));
		_searcher.start();// 手工启动

		_adapter.activate();
	}

	@Override
	public void stop() {
		_searcher.unregistry();
		_adapter.deactivate();
	}

}
