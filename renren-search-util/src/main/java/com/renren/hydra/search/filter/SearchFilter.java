package com.renren.hydra.search.filter;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;

public abstract class SearchFilter {
	protected String filterCondition;
	
	public SearchFilter(){
		
	}
	
	public SearchFilter(String filterCondition){
		this.filterCondition = filterCondition;
	}
	
	public abstract boolean filter(Long uid, OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData);
}
