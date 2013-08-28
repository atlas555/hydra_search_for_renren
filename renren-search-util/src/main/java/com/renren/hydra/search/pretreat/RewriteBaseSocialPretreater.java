package com.renren.hydra.search.pretreat;

import org.apache.lucene.search.Query;

import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.queryRewriter.IQueryRewriter;
import com.renren.hydra.search.queryRewriter.UserIdRestrictedQueryRewriter;

public class RewriteBaseSocialPretreater extends  DefaultSearchPretreater {
	private IQueryRewriter queryRewriter;
	
	public  RewriteBaseSocialPretreater(){
		this.queryRewriter = new UserIdRestrictedQueryRewriter();
	}

	@Override
	public Query queryPretreat(HydraRequest request) {
		return this.queryRewriter.rewrite(request.getQuery(), request);
	}

}
