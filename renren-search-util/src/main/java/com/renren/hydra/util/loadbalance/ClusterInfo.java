package com.renren.hydra.util.loadbalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.SearcherPrx;

public class ClusterInfo {
	private static Logger logger = Logger.getLogger(ClusterInfo.class);
	private int minSearchersPerPartition;
	private int maxSearchersPerPartition;
	private BrokerPrx[] brokers;
	private List<SearcherPrx>[] searchers;

	private Map<List<SearcherPrx>, List<String>> searchers2IdMap;
	private List<String> searchers2IdList;
	private List<String> brokers2IdList;
	private Map<SearcherPrx, List<String>>[] groupSearchers;

	public ClusterInfo() {

	}

	public int getMinSearchersPerPartition() {
		return minSearchersPerPartition;
	}

	public void setMinSearchersPerPartition(int minSearchersPerPartition) {
		if (minSearchersPerPartition < 0)
			this.minSearchersPerPartition = 0;
		this.minSearchersPerPartition = minSearchersPerPartition;
	}

	public int getMaxSearchersPerPartition() {
		return maxSearchersPerPartition;
	}

	public void setMaxSearchersPerPartition(int maxSearchersPerPartition) {
		if (maxSearchersPerPartition <= 0)
			this.maxSearchersPerPartition = 1;
		this.maxSearchersPerPartition = maxSearchersPerPartition;
	}

	public List<SearcherPrx>[] getSearchers() {
		return searchers;
	}

	public void setSearchers(List<SearcherPrx>[] searchers) {
		Map<List<SearcherPrx>, List<String>> idsMap = new HashMap<List<SearcherPrx>, List<String>>();
		List<String> idsList = new ArrayList<String>();
		List<String> tmpList = null;
		List<SearcherPrx> serverList = null;
		int minSearchersPerPartition = Integer.MAX_VALUE, maxSearchersPerPartition = 0;
		for (int i = 0; i < searchers.length; i++) {
			serverList = searchers[i];
			if (serverList == null || serverList.isEmpty())
				continue;
			tmpList = searcherPrxList2String(serverList);
			idsMap.put(serverList, tmpList);
			this.addWithExcluded(idsList, tmpList);
			if (tmpList.size() > maxSearchersPerPartition) {
				maxSearchersPerPartition = tmpList.size();
			}
			if (tmpList.size() < minSearchersPerPartition) {
				minSearchersPerPartition = tmpList.size();
			}
		}
		this.setMaxSearchersPerPartition(maxSearchersPerPartition);
		this.setMinSearchersPerPartition(minSearchersPerPartition);
		this.groupSearchers = groupSearchers(searchers, idsMap);
		this.searchers2IdMap = idsMap;
		this.searchers2IdList = idsList;
		this.searchers = searchers;
	}

	public BrokerPrx[] getBrokers() {
		return brokers;
	}

	public void setBrokers(BrokerPrx[] brokers) {
		this.brokers = brokers;
		List<String> idList = brokerPrxs2String(this.brokers);
		this.brokers2IdList = idList;
	}

	public List<String> searcherPrxs2String(List<SearcherPrx> searcherPrxList) {
		return searchers2IdMap.get(searcherPrxList);
	}

	public List<String> brokerPrx2String() {
		return brokers2IdList;
	}

	public List<String> searcherPrx2String() {
		return searchers2IdList;
	}

	public Map<SearcherPrx, List<String>>[] getGroupSearchers() {
		return this.groupSearchers;
	}

	private List<String> searcherPrxList2String(
			List<SearcherPrx> searcherPrxList) {
		List<String> idList = new ArrayList<String>();
		for (int i = 0; i < searcherPrxList.size(); i++) {
			idList.add(LoadBalanceUtil.searcherPrx2Id(searcherPrxList.get(i)));
		}
		return idList;
	}

	private List<String> brokerPrxs2String(BrokerPrx[] brokers) {
		List<String> idList = new ArrayList<String>();
		for (int i = 0; i < brokers.length; i++) {
			idList.add(LoadBalanceUtil.brokerPrx2Id(brokers[i]));
		}
		return idList;
	}

	private void addWithExcluded(List<String> idsList, List<String> tmpList) {
		for (String id : tmpList) {
			if (!idsList.contains(id))
				idsList.add(id);
		}
	}

	private Map<SearcherPrx, List<String>>[] groupSearchers(
			List<SearcherPrx>[] searchers,
			Map<List<SearcherPrx>, List<String>> idsMap) {
		int groupNum = this.maxSearchersPerPartition;
		if (searchers == null || searchers.length == 0) {
			return null;
		}
		Map<SearcherPrx, List<String>>[] groupSearchers = new Map[groupNum];
		for (int groupId = 0; groupId < groupNum; groupId++) {
			Map<String, SearcherPrx> id2SearcherPrx = new HashMap<String, SearcherPrx>();
			List<SearcherPrx> searcherList = null;
			Map<SearcherPrx, List<String>> prx2parts = new HashMap<SearcherPrx, List<String>>();
			for (int i = 0; i < searchers.length; i++) {
				searcherList = searchers[i];
				if (searcherList == null || searcherList.isEmpty())
					continue;
				List<String> idList = idsMap.get(searcherList);
				SearcherPrx searcherPrx = alreadyGrouped(id2SearcherPrx, idList);
				if (searcherPrx == null) {
					int index = select(idList.size(), groupId);
					searcherPrx = searcherList.get(index);
					id2SearcherPrx.put(idList.get(index), searcherPrx);
				}
				List<String> parts = prx2parts.get(searcherPrx);
				if (parts == null) {
					parts = new ArrayList<String>();
					prx2parts.put(searcherPrx, parts);
				}
				parts.add(String.valueOf(i));
				logger.debug("partition " + i + " is added into searcherPrx ["
						+ searcherPrx
						+ "] and all the partition in this searcherPrx is "
						+ parts);
			}
			groupSearchers[groupId] = prx2parts;
			logger.info("group [" + groupId + "]=" + prx2parts);
		}

		return groupSearchers;
	}

	private SearcherPrx alreadyGrouped(Map<String, SearcherPrx> id2SearcherPrx,
			List<String> idList) {
		SearcherPrx searcherPrx = null;
		for (String id : idList) {
			searcherPrx = id2SearcherPrx.get(id);
			if (searcherPrx != null) {
				return searcherPrx;
			}
		}
		return null;
	}

	private int select(int size, int groupId) {
		if (groupId >= size) {
			return groupId % size;
		} else {
			return groupId;
		}
	}
}
