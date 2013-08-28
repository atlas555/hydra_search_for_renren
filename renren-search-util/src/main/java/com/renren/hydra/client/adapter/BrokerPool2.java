package com.renren.hydra.client.adapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import Ice.Communicator;
import Ice.InitializationData;
import Ice.ObjectPrx;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.BrokerPrxHelper;
import com.renren.hydra.thirdparty.zkmanager2.Broker;
import com.renren.hydra.thirdparty.zkmanager2.Node;
import com.renren.hydra.thirdparty.zkmanager2.NodeChildListener;
import com.renren.hydra.thirdparty.zkmanager2.NodeDataListener;
import com.renren.hydra.thirdparty.zkmanager2.ZkCallback;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.hydra.util.loadbalance.ClusterInfo;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil;
import com.renren.hydra.util.loadbalance.Policy;
import com.renren.hydra.util.SearcherUtil;

public class BrokerPool2 {
	private static Logger logger = Logger.getLogger(BrokerPool2.class);
	private Communicator ic = null;

	private Policy policy;
	private ZkManager zkManager;
	private String business;

	private ClusterInfo clusterInfo = new ClusterInfo();
	private Map<String, String> path2ServerIdMap = new ConcurrentHashMap<String, String>();

	public BrokerPool2(String business, String zkAddress, Policy policy) {
		this.business = business;
		this.zkManager = ZkManager.getInstance(zkAddress);
		this.policy = policy;
	}

	public synchronized void init() {
		if (clusterInfo.getBrokers() != null
				&& clusterInfo.getBrokers().length != 0) {
			return;
		}
		iceInit();
		zkManager.subscribeChildChanges(Broker.getParentPath(),
				new NodeChildListener(new BrokerUpdater()));
		if (zkManager.exists(Broker.getParentPath())) {
			node2Proxy();
		}
	}

	private void iceInit() {
		InitializationData data = new InitializationData();
		Ice.Properties properties = Ice.Util.createProperties();
		properties.setProperty("Ice.MessageSizeMax", "10240");
		data.properties = properties;
		ic = Ice.Util.initialize(data);
	}

	public BrokerPrx getBroker(Map<String, String> userInfo) {
		if (clusterInfo.getBrokers() == null
				|| clusterInfo.getBrokers().length == 0) {
			init();
		}
		if (clusterInfo.getBrokers() == null
				|| clusterInfo.getBrokers().length == 0)
			return null;
		return policy.getBrokerPrx(clusterInfo, userInfo);
	}

	private void node2Proxy() {
		List<Node> nodes = zkManager.getNodes(Broker.getParentPath(), business);
		if (nodes == null || nodes.isEmpty()) {
			path2ServerIdMap.clear();
			return;
		}

		BrokerPrx[] tmpBrokers = new BrokerPrx[nodes.size()];
		String path;
		for (int i = 0; i < nodes.size(); i++) {
			Broker info = (Broker) nodes.get(i);
			if (!info.isAlive()) {
				continue;
			}
			BrokerPrx prx = getBrokerPrx(info);
			path = info.getAbsolutePath();
			if (!path2ServerIdMap.containsKey(path)) {
				zkManager.subscribeDataChanges(path, new NodeDataListener(
						new BrokerDataListener()));
				path2ServerIdMap.put(path, LoadBalanceUtil.brokerPrx2Id(prx));
			}
			tmpBrokers[i] = prx;
		}

		clusterInfo.setBrokers(tmpBrokers);

	}

	public BrokerPrx getBrokerPrx(Broker info) {
		String name = info.getBusiness();
		String endpoints = SearcherUtil.createEndpoints(info.getIp(),
				info.getPort());
		ObjectPrx prx = ic.stringToProxy(name + ":" + endpoints);
		BrokerPrx broker = BrokerPrxHelper.uncheckedCast(prx);
		return broker;
	}

	private void updateBrokerTimer(String dataPath, double time) {
		String serverId = path2ServerIdMap.get(dataPath);
		if (serverId != null)
			policy.updateTimer(path2ServerIdMap.get(dataPath), time);
	}

	private class BrokerUpdater implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) {
			node2Proxy();
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
		}

		@Override
		public void handleDataDeleted(String dataPath) {
		}
	}

	private class BrokerDataListener implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) {
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
			Node node = (Node) data;
			if (node != null)
				updateBrokerTimer(dataPath, node.getQPS());
			else
				updateBrokerTimer(dataPath, 0.0);
		}

		@Override
		public void handleDataDeleted(String dataPath) {
			logger.debug("data has been deleted from path '" + dataPath
					+ "' in zookeeper .");
			updateBrokerTimer(dataPath, 0.0);
			path2ServerIdMap.remove(dataPath);
		}

	}

}
