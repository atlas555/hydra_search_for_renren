package com.renren.hydra.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import Ice.InitializationData;
import Ice.ObjectPrx;

import com.renren.hydra.SearcherPrx;
import com.renren.hydra.SearcherPrxHelper;
import com.renren.hydra.config.HydraConfParams;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.thirdparty.zkmanager2.Node;
import com.renren.hydra.thirdparty.zkmanager2.NodeChildListener;
import com.renren.hydra.thirdparty.zkmanager2.NodeDataListener;
import com.renren.hydra.thirdparty.zkmanager2.Searcher;
import com.renren.hydra.thirdparty.zkmanager2.ZkCallback;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.hydra.util.SearcherUtil;
import com.renren.hydra.util.loadbalance.ClusterInfo;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil;
import com.renren.hydra.util.loadbalance.Policy;

public class SearcherPool2 {
	private static Logger logger = Logger.getLogger(SearcherPool2.class);

	// business -> partitions -> searchers
	// 保存的是searcher的prx, 由于不区分业务,这里保存了所有的searcher的信息
	private ZkManager zkManager;

	private Ice.Communicator ic = null;
	private String business;

	private Policy policy;

	private ClusterInfo clusterInfo = new ClusterInfo();

	private Map<String, String> path2ServerIdMap = new ConcurrentHashMap<String, String>();

	public SearcherPool2() {
	}

	public void init(String business, Policy policy) {
		this.policy = policy;
		init(business);
	}

	private void init(String business) {
		if (clusterInfo.getSearchers() != null
				&& clusterInfo.getSearchers().length != 0) {
			return;
		}
		this.business = business;
		iceInit();
		zkManager = ZkManager.getInstance(HydraConfig.getInstance()
				.getZkProperties().getString("address"));
		zkManager.subscribeChildChanges(Searcher.getParentPath(),
				new NodeChildListener(new SearcherListener()));

		if (zkManager.exists(Searcher.getParentPath()))
			proxyInit(business);
	}

	private void proxyInit(String business) {
		List<Node> nodes = zkManager.getNodes(Searcher.getParentPath(),
				business);
		int partitionSize = HydraConfig.getInstance().getHydraConfig()
				.getInt(HydraConfParams.PARTITION_SIZE, 0);
		node2Proxy(policy.getDisperseNode(nodes, partitionSize));
	}

	private void iceInit() {
		InitializationData data = new InitializationData();
		Ice.Properties properties = Ice.Util.createProperties();
		// properties.setProperty("Ice.MessageSizeMax", "10240");
		data.properties = properties;
		ic = Ice.Util.initialize(data);
	}

	public Map<SearcherPrx, List<String>> getSearcher2Partitions(
			Map<String, String> userInfo) {
		return policy.getSearcherPrx2Partitions(clusterInfo, userInfo);
	}

	// 对于GroupStrategy,nodes的key是group，value是这个group下的所有node
	// 对于PartitionStrategy,nodes的key是Partitions,values是node
	private void node2Proxy(List<Node>[] nodeListArray) {
		if (nodeListArray == null || nodeListArray.length == 0) {
			path2ServerIdMap.clear();
			return;
		}
		StringBuilder sb = new StringBuilder();
		sb.append("print node2Proxy array: {");
		List<SearcherPrx>[] searchers = new ArrayList[nodeListArray.length];
		for (int i = 0; i < nodeListArray.length; i++) {
			List<Node> value = nodeListArray[i];
			if (value == null || value.isEmpty())
				continue;
			sb.append("partition ");
			sb.append(i);
			sb.append(" : ");
			List<SearcherPrx> proxys = new ArrayList<SearcherPrx>();
			searchers[i] = proxys;
			String path = null;
			SearcherPrx searcherPrx = null;
			for (Node node : value) {
				searcherPrx = getProxy(node);
				proxys.add(searcherPrx);
				path = ((Searcher) node).getAbsolutePath();
				if (!path2ServerIdMap.containsKey(path)) {
					logger.info("start to subscribeDataChanges from zookeeper with path '"
							+ path + "'.");
					zkManager.subscribeDataChanges(path, new NodeDataListener(
							new SearcherDataListener()));
					path2ServerIdMap.put(path,
							LoadBalanceUtil.searcherPrx2Id(searcherPrx));
				}
				sb.append("(");
				sb.append(node.toString());
				sb.append(")");
			}
			sb.append("; ");
		}
		sb.append("}");
		logger.info(sb.toString());
		clusterInfo.setSearchers(searchers);
	}

	private SearcherPrx getProxy(Node node) {
		Searcher info = (Searcher) node;
		if (!info.isAlive() || info.isEmpty()) {
			return null;
		}
		String endpoints = SearcherUtil.createEndpoints(info.getIp(),
				info.getPort());
		ObjectPrx base = ic.stringToProxy(info.getBusiness() + ":" + endpoints);
		SearcherPrx prx = SearcherPrxHelper.uncheckedCast(base);
		return prx;
	}

	private void updateSearcherTimer(String dataPath, double time) {
		String serverId = path2ServerIdMap.get(dataPath);
		if (serverId != null)
			policy.updateTimer(serverId, time);
	}

	private class SearcherListener implements ZkCallback {
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

	private class SearcherDataListener implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) {
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
			logger.debug("data has been changed in path '" + dataPath
					+ "' in zookeeper .");
			Node node = (Node) data;
			if (node != null)
				updateSearcherTimer(dataPath, node.getQPS());
			else
				updateSearcherTimer(dataPath, 0.0);
		}

		@Override
		public void handleDataDeleted(String dataPath) {
			logger.debug("data has been deleted from path '" + dataPath
					+ "' in zookeeper .");
			updateSearcherTimer(dataPath, 0.0);
			path2ServerIdMap.remove(dataPath);
		}

	}

}
