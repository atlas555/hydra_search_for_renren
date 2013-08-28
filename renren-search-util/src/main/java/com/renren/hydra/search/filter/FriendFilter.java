package com.renren.hydra.search.filter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;

public class FriendFilter extends SearchFilter{
	
	private Map<MutableInt,Short> friends;
	private MutableInt friendIdTmp;
	
	public FriendFilter(Map<MutableInt,Short> friendsInfo){
		if(friendsInfo==null)
			this.friends = new HashMap<MutableInt,Short>();
		else
			this.friends = friendsInfo;
		this.friendIdTmp = new MutableInt(0);
	}
	

	@Override
	public boolean filter(Long uid, OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) {
		int userIdFound = onlineAttributeData.getUserId();
		friendIdTmp.setValue(userIdFound);
		if (friends.containsKey(friendIdTmp)) 
			return true;
		return false;
	}

}
