package com.renren.hydra.util.loadbalance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.SearcherPrx;

public class GroupPolicy extends AbstractPolicy {

	private static Logger logger = Logger.getLogger(GroupPolicy.class);

	public GroupPolicy() {
	}

	protected int select(int size, int groupId) {
		if (groupId >= size) {
			return groupId % size;
		} else {
			return groupId;
		}
	}

	@Override
	public Map<SearcherPrx, List<String>> getSearcherPrx2Partitions(
			ClusterInfo clusterInfo, Map<String, String> userInfo) {
		Map<SearcherPrx, List<String>>[] groupSearchers = clusterInfo
				.getGroupSearchers();
		if (groupSearchers == null || groupSearchers.length == 0)
			return new HashMap<SearcherPrx, List<String>>();
		int groupId = Integer.valueOf(userInfo.get("USER_ID"))
				% clusterInfo.getMaxSearchersPerPartition();
		if (groupId >= groupSearchers.length) {
			groupId = groupId % groupSearchers.length;
		}
		return groupSearchers[groupId];

	}

	public BrokerPrx getBrokerPrx(ClusterInfo clusterInfo,
			Map<String, String> userInfo) {
		BrokerPrx[] brokers = clusterInfo.getBrokers();
		if (brokers == null || brokers.length == 0)
			return null;
		return brokers[select(brokers.length,
				Integer.valueOf(userInfo.get("USER_ID")) % brokers.length)];
	}

	@Override
	public void updateTimer(String serverId, double time) {
		
	}

}
