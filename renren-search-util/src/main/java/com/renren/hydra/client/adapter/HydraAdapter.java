package com.renren.hydra.client.adapter;

import java.util.HashMap;
import java.util.Map;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.client.Condition;
import com.renren.hydra.search.HydraResult;
import com.renren.hydra.util.SerializableTool;
import com.renren.hydra.util.loadbalance.Policy;
import com.renren.hydra.util.loadbalance.PolicyFactory;

public class HydraAdapter {
	private static Map<String, HydraAdapter> instances = new HashMap<String, HydraAdapter>();
	private BrokerPool2 brokerPool;
	private String business;
	private Policy policy;

	public static HydraAdapter getInstance(String business) {
		HydraAdapter instance = instances.get(business);
		if (instance == null) {
			synchronized (instances) {
				// double check
				instance = instances.get(business);
				if (instance == null) {
					instance = new HydraAdapter(business);
					instances.put(business, instance);
				}
			}
		}
		return instance;
	}

	private HydraAdapter(String business) {
		this.business = business;
	}

	public synchronized void init(String zkAddress) {
		String loadBalance = null;
		this.init(zkAddress, loadBalance);
	}

	public synchronized void init(String zkAddress, String loadBalance) {
		if (this.policy != null && this.brokerPool != null)
			return;
		Policy policy = PolicyFactory.createPolicy(loadBalance);
		this.policy = policy;
		brokerPool = new BrokerPool2(this.business, zkAddress, policy);
	}

	public HydraResult search(Condition condition, int begin, int limit) {
		if (null == this.brokerPool) {
			return HydraResult.EMPTY_RESULT;
		}
		Map<String, String> userInfo = condition.getUserInfo();
		if (userInfo == null) {
			userInfo = new HashMap<String, String>();
			condition.setUserInfo(userInfo);
		}
		userInfo.put("USER_ID", String.valueOf(condition.getUserId()));
		BrokerPrx brokerPrx = this.brokerPool.getBroker(userInfo);
		byte[] req = SerializableTool.objectToBytes(condition);
		byte[] bytes = brokerPrx.search(req, begin, limit);

		HydraResult result = (HydraResult) SerializableTool
				.bytesToObject(bytes);
		return result;
	}
}
