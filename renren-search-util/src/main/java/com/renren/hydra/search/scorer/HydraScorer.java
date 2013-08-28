package com.renren.hydra.search.scorer;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Similarity;

public abstract class HydraScorer extends Scorer {
	public HydraScorer(Similarity similarity) {
		super(similarity);
	}

	public abstract TermMatchInfo[] getMatchInfos();
	public abstract long getMatchFlags();
	public abstract int getTotalTermCount();
	public abstract int getMatchTermCount();
}
