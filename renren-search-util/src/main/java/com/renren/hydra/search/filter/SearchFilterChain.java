package com.renren.hydra.search.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.util.SearchFlowFactory;
import org.apache.commons.collections.map.LRUMap;

public class SearchFilterChain{
	private static Logger logger = Logger.getLogger(SearchFilterChain.class);
	private final static int CACHE_SIZE = 1000;
	private static Map<FilterInfo, SearchFilter> filterCache = new LRUMap(CACHE_SIZE);//<FilterInfo, SearchFilter>();
	
	private List<SearchFilter> searchFilters=null;
	private Filter filter;
	private int filterNum=0;
	
	public SearchFilterChain(Filter filter){
		this.filter=filter;
		if(filter!=null){
			FilterInfo[] filterInfos = filter.getFilterInfos();
			if(filterInfos!=null){
				int cnt = filterInfos.length;
				List<SearchFilter> searchFilterList = new ArrayList<SearchFilter>(cnt);
				for(int i=0;i<cnt;++i){
					FilterInfo filterInfo = filterInfos[i];
					SearchFilter searchFilter = getFilter(filterInfo);
					if(searchFilter!=null)
						searchFilterList.add(searchFilter);
				}
				this.filterNum = searchFilterList.size();
				this.searchFilters = searchFilterList;
			}
		}
	}
	
	public SearchFilterChain(){
		this.searchFilters = new ArrayList<SearchFilter>();
		this.filterNum = 0;
	}
	
	public void addSearchFilter(SearchFilter filter){
		this.searchFilters.add(filter);
		++this.filterNum;
	}

	/*
	 * 有一个filter不满足条件，则返回false
	 * 都满足条件，则返回true
	 */
	
	public boolean filter(Long uid, OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) {
		for(int i=0;i<this.filterNum;++i){
			if(!this.searchFilters.get(i).filter(uid,onlineAttributeData, offlineAttributeData))
				return false;
		}
		return true;
	}

	public static SearchFilter getFilter(FilterInfo filterInfo) {
		Schema schema = Schema.getInstance();
		SearchFilter searchFilter = null;
		synchronized(filterCache){
			searchFilter = filterCache.get(filterInfo);
			if (searchFilter == null) {
				searchFilter = SearchFlowFactory.createSearchFilter(schema, filterInfo);
			if(searchFilter!=null)
				logger.debug("add filter "+ filterInfo.toString() +"to filterCache");
				filterCache.put(filterInfo, searchFilter);
			}
			return searchFilter;
		}
	}
}
