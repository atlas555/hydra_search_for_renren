package com.renren.hydra.search.parser;

import java.util.*;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Explanation;

import com.renren.hydra.search.scorer.HydraBooleanScorer;
import com.renren.hydra.search.scorer.HydraScorer;

public class HydraBooleanQuery extends BooleanQuery {


	private static final long serialVersionUID = 1L;

	public HydraBooleanQuery() {}

	public HydraBooleanQuery(boolean disableCoord) {
		super(disableCoord);
	}

	protected class HydraBooleanWeight extends Weight {
		private static final long serialVersionUID = 1L;

		
		protected List<BooleanClause> clauses;
		protected Similarity similarity;
		protected ArrayList<Weight> weights;

		public HydraBooleanWeight(Searcher searcher, List<BooleanClause> clauses) throws IOException {
			this.clauses = clauses;
			this.similarity = getSimilarity(searcher);
			weights = new ArrayList<Weight>(clauses.size());
			for (int i = 0 ; i < clauses.size(); i++) {
				weights.add(clauses.get(i).getQuery().createWeight(searcher));
			}
		}
	
		@Override
		public Query getQuery() { return HydraBooleanQuery.this; }

		@Override
		public float getValue() { return getBoost(); }

		@Override
		public float sumOfSquaredWeights() throws IOException {
			return 0.0f;
		}
	
		@Override
		public void normalize(float norm) { }
	
		@Override
		public Explanation explain(IndexReader reader, int doc) throws IOException {
			return new Explanation();
		}

		@Override
		public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer)
				throws IOException {
			List<HydraScorer> required = new ArrayList<HydraScorer>();
			List<HydraScorer> prohibited = new ArrayList<HydraScorer>();
			List<HydraScorer> optional = new ArrayList<HydraScorer>();
			Iterator<BooleanClause> cIter = clauses.iterator();
			for (Weight w  : weights) {
				BooleanClause c =  cIter.next();
				HydraScorer subScorer = (HydraScorer)w.scorer(reader, true, false);
				if (subScorer == null) {
					if (c.isRequired()) {
						return null;
					}
				} else if (c.isRequired()) {
					required.add(subScorer);
				} else if (c.isProhibited()) {
					prohibited.add(subScorer);
				} else {
					optional.add(subScorer);
				}
			}
  
		  	if (required.size() == 0 && optional.size() == 0) {
			  	return null;
		  	} 
  	
			return new HydraBooleanScorer(similarity, minNrShouldMatch, required, prohibited, optional);
	  	}

	  	@Override
	  	public boolean scoresDocsOutOfOrder() {
		  	return false;
	  	}
	}
  
	@Override
	public Weight createWeight(Searcher searcher) throws IOException {
		return new HydraBooleanWeight(searcher, clauses());
	} 
}
