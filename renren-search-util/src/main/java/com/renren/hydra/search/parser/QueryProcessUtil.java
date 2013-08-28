package com.renren.hydra.search.parser;

import org.apache.log4j.Logger;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;

public class QueryProcessUtil {

	public static void processPhraseQuery(Query rootQuery, int slop) {
		if (!(rootQuery instanceof BooleanQuery))
			return;
		BooleanClause[] clauses = ((BooleanQuery) rootQuery).getClauses();
		for (BooleanClause clause : clauses) {
			Query subQuery = clause.getQuery();
			if (subQuery instanceof PhraseQuery) {
				PhraseQuery pq = (PhraseQuery) subQuery;
				pq.setSlop(slop);
			} else if (subQuery instanceof BooleanQuery) {
				processPhraseQuery(subQuery,slop);
			} else {
				continue;
			}
		}
	}
}
