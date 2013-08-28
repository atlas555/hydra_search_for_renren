package com.renren.hydra.search.parser;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;

public class HydraQueryParser extends QueryParser {
	private int termIndex = 0;
	
	public HydraQueryParser(Version matchVersion, String field, Analyzer analyzer) {
		super(matchVersion, field, analyzer);
	}

	public int getTotalTermCount() {
		return termIndex;
	}

	public void setTotalTermCount(int termIndex) {
		this.termIndex = termIndex;
	}

	@Override
	protected Query newTermQuery(Term term){
		Query query = new HydraTermQuery(term, termIndex);
		termIndex++;
		return query;
	}

	@Override
	protected BooleanQuery newBooleanQuery(boolean disableCoord) {
		return new HydraBooleanQuery(disableCoord);
	}

	@Override
	protected PhraseQuery newPhraseQuery(){
		PhraseQuery query = new HydraPhraseQuery(this);
		query.setSlop(Integer.MAX_VALUE);
		
		return query;
	}
}
