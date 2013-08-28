package com.renren.hydra.searcher.core.search.filter;


import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;

/*
 * 根据uid 和 Attribute 进行过滤，返回true 则过滤掉
 * 每一个查询的检索过程都要经过ConstantFilter的过滤, 例如SecurityFilter, 安全过滤
 */
public abstract class ConstantFilter {
	private String filterName;

	public String getFilterName() {
		return filterName;
	}

	public void setFilterName(String filterName) {
		this.filterName = filterName;
	}

	public ConstantFilter() {
	}

	public ConstantFilter(String filterName) {
		this.filterName = filterName;
	}

	public abstract boolean filter(long docId,OnlineAttributeData onlineAttributes, OfflineAttributeData offlineAttributes,Map<MutableInt,Short> friendsInfo);
}
