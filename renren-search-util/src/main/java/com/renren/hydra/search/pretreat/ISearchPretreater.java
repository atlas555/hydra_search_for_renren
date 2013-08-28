package com.renren.hydra.search.pretreat;

import org.apache.lucene.search.Query;

import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.filter.SearchFilterChain;
import com.renren.hydra.search.sort.Sort;

public interface ISearchPretreater {
	public Query queryPretreat(HydraRequest request);
	public SearchFilterChain filterPretreat(HydraRequest request);
	public Sort SortPretreat(HydraRequest request);
}
