package com.renren.hydra.service;

import java.util.Date;
import java.io.File;
import org.apache.log4j.*;

import xce.util.tools.IPAddress;
import Ice.Communicator;
import Ice.Properties;
import IceBox.Service;

import com.renren.hydra.broker.BrokerI;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.util.SearcherUtil;

public class BrokerService implements Service{
	private Ice.ObjectAdapter _adapter;
	private static Logger logger = Logger.getLogger(BrokerService.class);
	private BrokerI broker;
	
	@Override
	public void start(String name, Communicator ic, String[] args){
		Date date = new Date();
		Properties p = ic.getProperties();
		String confDir = p.getPropertyWithDefault("Config", "../etc/classes/node/");
		String business = p.getPropertyWithDefault("BusinessName", "Broker");
		File conf = new File(confDir);
		if (!conf.exists()) {
			logger.error("config dir " + confDir + "does not exist!");
			System.out.println("config dir " + confDir + "does not exist!");
			return;
		}

		HydraConfig config = HydraConfig.getInstance();
		if (!config.init(confDir)) {
			logger.error("fail to start BrokerService " + date.toString());
			return;
		}
		                
		logger.info("start BrokerService " + date.toString());
		
		String ip = IPAddress.getLocalAddress();
		String port = p.getPropertyWithDefault("Port", "20028");
		String endpoints = SearcherUtil.createEndpoints(ip, port);

		_adapter = ic.createObjectAdapterWithEndpoints(name, endpoints);
		broker = new BrokerI(config, ip, port, business);		
		if (!broker.init()) {
			logger.error("fail to init BrokerI.");
			return;
		}

		_adapter.add(broker, ic.stringToIdentity(business));		
		broker.registry();
		_adapter.activate();
	}

	@Override
	public void stop() {
		broker.unregistry();
		_adapter.deactivate();
	}

}

