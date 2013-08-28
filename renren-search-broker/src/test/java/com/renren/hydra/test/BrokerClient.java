package com.renren.hydra.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;

import Ice.InitializationData;
import Ice.ObjectPrx;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.BrokerPrxHelper;
import com.renren.hydra.thirdparty.zkmanager2.Broker;
import com.renren.hydra.thirdparty.zkmanager2.Node;
import com.renren.hydra.thirdparty.zkmanager2.NodeChildListener;
import com.renren.hydra.thirdparty.zkmanager2.ZkCallback;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.hydra.util.SearcherUtil;
import com.renren.hydra.util.loadbalance.Policy;
import com.renren.hydra.util.loadbalance.PolicyFactory;

public class BrokerClient {
	private static Logger logger = Logger.getLogger(BrokerClient.class);

	// business -> partitions -> searchers
	// 保存的是searcher的prx, 由于不区分业务,这里保存了所有的searcher的信息
	private static Map<String, List<BrokerPrx>> pool = new ConcurrentHashMap<String, List<BrokerPrx>>();

	private static BrokerClient instance = new BrokerClient();

	private Ice.Communicator ic = null;
	private String business;

	private Policy policy;

	private ZkManager zkManager;
	
	private BrokerClient() {
		init("share");
	}

	public static BrokerClient getInstance() {
		return instance;
	}

	public synchronized void init(String business) {

		init(business, PolicyFactory.createPolicy("RESPONSE_TIME"));
	}

	private synchronized void init(String business, Policy policy) {
		if (!pool.isEmpty()) {
			return;
		}
		this.business = business;
		this.policy = policy;
		iceInit();
		proxyInit(business);
		zkManager = ZkManager.getInstance("localhost:2181");
		zkManager.subscribeChildChanges(
				new Broker().getParentPath(),
				new NodeChildListener(new BrokerListener()));
	}

	private void proxyInit(String business) {
		List<Node> nodes = zkManager.getNodes(
				(new Broker()).getParentPath(), business);
		Map<String, List<Node>> map = new HashMap<String, List<Node>>();
		map.put("0", nodes);
		System.out.println(map);
		node2Proxy(map);
	}

	private void iceInit() {
		InitializationData data = new InitializationData();
		Ice.Properties properties = Ice.Util.createProperties();
		properties.setProperty("Ice.MessageSizeMax", "10240");
		data.properties = properties;
		ic = Ice.Util.initialize(data);
	}

//	public Map<BrokerPrx, List<String>> getBroker2Partitions() {
//		return policy.getBrokerPrx2Partitions(pool);
//	}
//
//	public Map<BrokerPrx, List<String>> getBroker2Partitions(String business) {
//		return policy.getBrokerPrx2Partitions(pool);
//	}

	// 对于GroupStrategy,nodes的key是group，value是这个group下的所有node
	// 对于PartitionStrategy,nodes的key是Partitions,values是node
	private void node2Proxy(Map<String, List<Node>> nodes) {
		if (nodes == null) {
			return;
		}

		StringBuilder sb = new StringBuilder();
		sb.append("print node2Proxy map: {");
		Map<String, List<BrokerPrx>> map = new ConcurrentHashMap<String, List<BrokerPrx>>();
		for (Entry<String, List<Node>> entry : nodes.entrySet()) {
			String key = entry.getKey();
			sb.append("partition ");
			sb.append(key);
			sb.append(" : ");
			List<Node> value = entry.getValue();
			List<BrokerPrx> proxys = map.get(key);
			if (proxys == null) {
				proxys = new ArrayList<BrokerPrx>();
				map.put(key, proxys);
			}

			for (Node node : value) {
				proxys.add(getProxy(node));
				sb.append("(");
				sb.append(node.toString());
				sb.append(")");
			}

			sb.append("; ");
		}
		sb.append("}");
		logger.info(sb.toString());

		synchronized (pool) {
			pool.clear();
			pool.putAll(map);
		}
	}

	private BrokerPrx getProxy(Node node) {
		Broker info = (Broker) node;
		if (!info.isAlive() || info.isEmpty()) {
			return null;
		}
		String name = SearcherUtil.createAdapterName(info.getBusiness()/*
																		 * ,
																		 * info.
																		 * getGroupId
																		 * ()
																		 */);
		String endpoints = SearcherUtil.createEndpoints(info.getIp(),
				info.getPort());
		ObjectPrx base = ic.stringToProxy(info.getBusiness() + ":" + endpoints);
		BrokerPrx prx = BrokerPrxHelper.uncheckedCast(base);
		return prx;
	}

	private class BrokerListener implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) {
			logger.info("reinit proxy.");
			proxyInit(business);
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
		}

		@Override
		public void handleDataDeleted(String dataPath) {
		}

	}

	public static void main(String[] args) throws Exception {
		ZkConnection connection = new ZkConnection("10.3.17.72:2181");
		ZkClient client = new ZkClient(connection);
		client.writeData("/search2/security_data", "test");
		System.exit(0);
	}
}
