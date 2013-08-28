package com.renren.hydra.search.pretreat;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.renren.hydra.client.SearchType;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.filter.Filter;
import com.renren.hydra.search.filter.FriendFilter;
import com.renren.hydra.search.filter.OwnerFilter;
import com.renren.hydra.search.filter.SearchFilter;
import com.renren.hydra.search.filter.SearchFilterChain;
import com.renren.hydra.util.FriendInfoUtil;

public class FilterBaseSocialPretreater extends DefaultSearchPretreater{

	@Override
	public SearchFilterChain filterPretreat(HydraRequest request) {
		SearchFilterChain searchFilterChain = null;
		Filter filter = request.getFilter();
		if(filter!=null){
			searchFilterChain = new SearchFilterChain(filter);
		}
		SearchType _searchType = request.getSearchType();
		SearchFilter searchFilter = null;
		if(_searchType == SearchType.OnlyUser){
			 searchFilter = new OwnerFilter(request.getUserId());
		}else if(_searchType == SearchType.OnlyFriends){
			Map<Integer,Map<MutableInt,Short>> friendsInfo = request.getFriendsInfoFV();
			if(friendsInfo==null)
				searchFilter = new FriendFilter(null);
			else
				searchFilter = new FriendFilter(friendsInfo.get(1));
		}else{
			searchFilter = null;
		}
		
		if(searchFilter!=null){
			if(searchFilterChain==null)
				searchFilterChain = new SearchFilterChain();
			searchFilterChain.addSearchFilter(searchFilter);
		}
		return searchFilterChain;
	}

}
