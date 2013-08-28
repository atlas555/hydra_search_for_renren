package com.renren.hydra.search.pretreat;

import org.apache.lucene.search.Query;

import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.filter.SearchFilterChain;
import com.renren.hydra.search.sort.Sort;

public class DefaultSearchPretreater implements ISearchPretreater{

	public DefaultSearchPretreater(){
		
	}
	
	@Override
	public Sort SortPretreat(HydraRequest request) {
		return request.getSort();
	}

	@Override
	public SearchFilterChain filterPretreat(HydraRequest request) {
		return new SearchFilterChain(request.getFilter());
	}

	@Override
	public Query queryPretreat(HydraRequest request) {
		return request.getQuery();
	}

}
