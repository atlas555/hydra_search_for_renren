package com.renren.hydra.util.loadbalance;

import java.util.List;
import java.util.Map;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.SearcherPrx;
import com.renren.hydra.thirdparty.zkmanager2.Node;
import com.renren.hydra.util.loadbalance.ClusterInfo;

public interface Policy {
	public List<Node>[] getDisperseNode(List<Node> nodes, int partionSize);

	public Map<SearcherPrx, List<String>> getSearcherPrx2Partitions(
			ClusterInfo clusterInfo, Map<String, String> userInfo);

	public BrokerPrx getBrokerPrx(ClusterInfo clusterInfo,
			Map<String, String> userInfo);

	public void updateTimer(String serverId, double time);
}
