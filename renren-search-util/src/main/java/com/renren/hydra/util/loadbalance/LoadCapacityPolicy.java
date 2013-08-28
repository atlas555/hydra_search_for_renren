package com.renren.hydra.util.loadbalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.SearcherPrx;

public class LoadCapacityPolicy extends AbstractPolicy {
	private static Logger logger = Logger.getLogger(LoadCapacityPolicy.class);

	private Map<String, Statistic> statisticsMap = new ConcurrentHashMap<String, Statistic>();

	public LoadCapacityPolicy() {
	}

	private static class Statistic {
		private double QPS;
	}

	protected int select(List<String> idList) {
		int index = 0;
		double times = Double.MAX_VALUE;
		Statistic tmp = null;
		for (int i = 0; i < idList.size(); i++) {
			tmp = statisticsMap.get(idList.get(i));
			if (tmp == null) {
				return i;
			}
			if (tmp.QPS < times) {
				times = tmp.QPS;
				index = i;
			}
		}
		return index;
	}

	@Override
	public Map<SearcherPrx, List<String>> getSearcherPrx2Partitions(
			ClusterInfo clusterInfo, Map<String, String> userInfo) {
		long startTime = System.currentTimeMillis();
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
			SearcherPrx searcherPrx = searcherList.get(select(clusterInfo
					.searcherPrxs2String(searcherList)));
			List<String> parts = prx2parts.get(searcherPrx);
			if (parts == null) {
				parts = new ArrayList<String>();
				prx2parts.put(searcherPrx, parts);
			}
			parts.add(String.valueOf(i));
		}
		logger.debug("Wasted time of getSearcherPrx2Partitions is "
				+ (System.currentTimeMillis() - startTime) + "(ms)");
		return prx2parts;
	}

	public BrokerPrx getBrokerPrx(ClusterInfo clusterInfo,
			Map<String, String> userInfo) {
		BrokerPrx[] brokers = clusterInfo.getBrokers();
		if (brokers == null || brokers.length == 0)
			return null;
		return brokers[select(clusterInfo.brokerPrx2String())];
	}

	@Override
	public void updateTimer(String serverId, double QPS) {
		if (QPS == 0.0) {
			statisticsMap.remove(serverId);
		} else {
			Statistic statistic = statisticsMap.get(serverId);
			if (statistic == null) {
				statistic = new Statistic();
				statisticsMap.put(serverId, statistic);
			}
			statistic.QPS = QPS;
		}

	}
}
