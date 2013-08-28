package com.renren.hydra.search.queryRewriter;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;

import com.renren.hydra.client.SearchType;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.parser.HydraBooleanQuery;
import com.renren.hydra.search.parser.HydraTermQuery;
import com.renren.hydra.util.FriendInfoUtil;

public class UserIdRestrictedQueryRewriter implements IQueryRewriter{
	private static final Logger logger = Logger.getLogger(UserIdRestrictedQueryRewriter.class);
	
	@Override
	public Query rewrite(Query query, HydraRequest request) {
		SearchType searchType = request.getSearchType();
		logger.debug("searchType:"+searchType.toString());
		if(searchType != SearchType.OnlyFriends && searchType != SearchType.OnlyUser)
			return query;
		
		
		Query q = request.getQuery();

		Schema schema = Schema.getInstance();
		BooleanQuery retQuery = new HydraBooleanQuery();
		if(!(q instanceof MatchAllDocsQuery))
			retQuery.add(q, Occur.MUST);
		
		String userIdField = schema.getUserIdFieldName();
		// 搜自己
		if (searchType == SearchType.OnlyUser) {
			logger.debug("only user");
			String userId = String.valueOf(request.getUserId());
			Term term = new Term(userIdField, userId);
			HydraTermQuery tq = new HydraTermQuery(term, 0);
			retQuery.add(tq, Occur.MUST);
		}//搜好友 
		else if (searchType  == SearchType.OnlyFriends) {
			logger.debug("only friend");
			Map<Integer,Map<MutableInt,Short>> twoDegreeFriendsInfo = request.getFriendsInfoFV();
			if (twoDegreeFriendsInfo!=null && twoDegreeFriendsInfo.containsKey(1)) {
				Map<MutableInt, Short> friendsInfo = twoDegreeFriendsInfo
						.get(1);
				Set<MutableInt> friends = friendsInfo.keySet();
				HydraBooleanQuery uidQuery = new HydraBooleanQuery();
				for (MutableInt i: friends) {
					String str = String.valueOf(i.getValue());
					Term term = new Term(userIdField, str);
					HydraTermQuery tq = new HydraTermQuery(term, 0);
					uidQuery.add(tq, Occur.SHOULD);
				}
				retQuery.add(uidQuery, Occur.MUST);
			}
		}
		logger.debug("query after rewrite:"+retQuery.toString());
		return retQuery;
	}

}
