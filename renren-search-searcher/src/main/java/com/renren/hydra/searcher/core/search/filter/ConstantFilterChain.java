package com.renren.hydra.searcher.core.search.filter;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;

/*
 * filter chain
 * 任何一个filter返回true，则返回true，否则返回false
 */
public class ConstantFilterChain {
	private static final Logger logger = Logger.getLogger(ConstantFilterChain.class);
	
	private ConstantFilter[] filters;
	private int size;
	private int num;

	public boolean addFilter(ConstantFilter f) {
		if (num < size) {
			filters[num] = f;
			++num;
			return true;
		}
		return false;
	}

	public ConstantFilterChain(int size) {
		this.size = size;
		filters = new ConstantFilter[this.size];
		num = 0;
	}

	// return filter num
	public int getFilterNum() {
		return this.num;
	}

	public boolean filter(long docId,OnlineAttributeData onlineAttributes, OfflineAttributeData offlineAttributes,Map<MutableInt,Short> friendsInfo) {
		for (int i = 0; i < num; ++i) {
			if (filters[i].filter(docId,onlineAttributes, offlineAttributes,friendsInfo))
				return true;
		}
		return false;
	}
}
