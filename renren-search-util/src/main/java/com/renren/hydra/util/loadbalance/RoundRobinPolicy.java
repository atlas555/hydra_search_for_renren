package com.renren.hydra.util.loadbalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.SearcherPrx;

public class RoundRobinPolicy extends AbstractPolicy {

	private AtomicLong loadBalanceTimes = new AtomicLong(0L);

	public RoundRobinPolicy() {
	}

	protected int select(int size) {
		return (int) loadBalanceTimes.getAndIncrement() % size;
	}

	@Override
	public Map<SearcherPrx, List<String>> getSearcherPrx2Partitions(
			ClusterInfo clusterInfo, Map<String, String> userInfo) {
		Map<SearcherPrx, List<String>> prx2parts = new HashMap<SearcherPrx, List<String>>();
		List<SearcherPrx>[] searchers = clusterInfo.getSearchers();
		if (searchers == null || searchers.length == 0) {
			return prx2parts;
		}
		List<SearcherPrx> searcherList = null;
		for (int i = 0; i < searchers.length; i++) {
			searcherList = searchers[i];
			if (searcherList == null || searcherList.isEmpty())
				continue;
			SearcherPrx searcherPrx = searcherList.get(select(searcherList
					.size()));
			List<String> parts = prx2parts.get(searcherPrx);
			if (parts == null) {
				parts = new ArrayList<String>();
				prx2parts.put(searcherPrx, parts);
			}
			parts.add(String.valueOf(i));
		}
		return prx2parts;
	}

	public BrokerPrx getBrokerPrx(ClusterInfo clusterInfo,
			Map<String, String> userInfo) {
		BrokerPrx[] brokers = clusterInfo.getBrokers();
		if (brokers == null || brokers.length == 0)
			return null;
		return brokers[select(brokers.length)];
	}

	@Override
	public void updateTimer(String serverId, double time) {
		
	}
}
