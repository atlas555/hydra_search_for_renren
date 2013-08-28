package com.renren.hydra.search.filter;

import org.apache.log4j.Logger;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;

public class OwnerFilter extends SearchFilter{
	
	public OwnerFilter(){
	}

	public OwnerFilter(String filterCondition) {
		super(filterCondition);
	}

	private int ownerId;
	public OwnerFilter(int ownerId){
		this.ownerId = ownerId;
	}

	@Override
	public boolean filter(Long uid, OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) {
		if(this.ownerId==onlineAttributeData.getUserId())
			return true;
		return false;
	}

}
