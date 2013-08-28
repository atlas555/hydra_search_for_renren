package com.renren.hydra.util.loadbalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.renren.hydra.thirdparty.zkmanager2.Node;
import com.renren.hydra.thirdparty.zkmanager2.Searcher;

public abstract class AbstractPolicy implements Policy {
	private static Logger logger = Logger.getLogger(AbstractPolicy.class);

	public AbstractPolicy() {
	}

	@Override
	public List<Node>[] getDisperseNode(List<Node> nodes, int partionSize) {
		if (nodes == null || nodes.isEmpty()) {
			return null;
		}
		List<Node>[] nodeListArray = new ArrayList[partionSize];
		for (Node node : nodes) {
			Searcher info = (Searcher) node;
			int[] partitions = info.getPartitions();
			if(partitions!=null){
				for (int pid : partitions) {
					List<Node> list = nodeListArray[pid];
					if (list == null) {
						list = new ArrayList<Node>();
						nodeListArray[pid] = list;
					}
					list.add(node);
				}
			}
		}
		return nodeListArray;
	}
}
