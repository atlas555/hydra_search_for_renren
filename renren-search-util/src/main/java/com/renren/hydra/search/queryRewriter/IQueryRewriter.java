package com.renren.hydra.search.queryRewriter;

import org.apache.lucene.search.Query;

import com.renren.hydra.search.HydraRequest;

public interface IQueryRewriter {
	public Query rewrite(Query query, HydraRequest request);
}
