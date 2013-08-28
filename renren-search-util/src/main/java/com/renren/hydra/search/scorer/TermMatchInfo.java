package com.renren.hydra.search.scorer;

import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.Term;

public class TermMatchInfo {
	public Term term = null;
	public int tf = -1;
	public TermPositions positions = null;
	public float idf = 0.0f;
	public int index = -1;

	public TermMatchInfo() {}

	public TermMatchInfo(TermMatchInfo matchInfo) {
		this.term = new Term(matchInfo.term.field(), matchInfo.term.text());
		this.tf = matchInfo.tf;
		this.positions = matchInfo.positions;
		this.idf = matchInfo.idf;
		this.index = matchInfo.index;
	}
}

