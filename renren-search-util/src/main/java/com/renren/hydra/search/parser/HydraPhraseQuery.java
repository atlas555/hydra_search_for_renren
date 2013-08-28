package com.renren.hydra.search.parser;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.Serializable;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;

import com.renren.hydra.search.scorer.HydraConjunctionScorer;
import com.renren.hydra.search.scorer.HydraScorer;

public class HydraPhraseQuery extends PhraseQuery implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private transient HydraQueryParser queryParser;
	private ArrayList<Integer> indexList = new ArrayList<Integer>(4);

	private class HydraPhraseWeight extends Weight {
		private static final long serialVersionUID = 1L;
		
		private Term[] terms;
		private int[] indexes;
		private Similarity similarity;
		private Searcher searcher;

		public HydraPhraseWeight(Searcher searcher, Term[] terms, int[] indexes) throws IOException {
			this.terms = terms;
			this.indexes = indexes;
			this.searcher = searcher;
			this.similarity = getSimilarity(searcher);
		}

		@Override
		public String toString() { return "weight(" + HydraPhraseQuery.this + ")"; }

		@Override
		public Query getQuery() { return HydraPhraseQuery.this; }

		@Override
		public float getValue() { return 0.0f; }

		@Override
		public float sumOfSquaredWeights() {
			return 0.0f;
		}

		@Override
		public void normalize(float queryNorm) {}

		@Override
		public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) 
			throws IOException {
			if (terms.length == 0)			  // optimize zero-term case
				return null;

			List<HydraScorer> required = new ArrayList<HydraScorer>();
			for (int i = 0; i < terms.length; i++) {
				Query termQuery = new HydraTermQuery(terms[i], indexes[i]);
	  			Weight weight = termQuery.createWeight(searcher);
				HydraScorer subScorer = (HydraScorer)weight.scorer(reader, true, false);
				required.add(subScorer);
			}
			return new HydraConjunctionScorer(similarity, required);
		}

		@Override
		public Explanation explain(IndexReader reader, int doc) {
    		return new Explanation();
    	}
  	}

	public HydraPhraseQuery(HydraQueryParser queryParser) {
		this.queryParser = queryParser;	
	}

	@Override
	public void add(Term term, int position) {
		super.add(term, position);
		int index = queryParser.getTotalTermCount();
		indexList.add(index);
		queryParser.setTotalTermCount(index + 1);
	}

  	@Override
  	public Weight createWeight(Searcher searcher) throws IOException {
  		Term[] termArray = getTerms();
		int[] indexArray = new int[termArray.length];
		int i = 0;
		for (Iterator<Integer> it = indexList.iterator(); it.hasNext();) {
			indexArray[i] = it.next();
			i++;
		}
		if (termArray.length == 1) {			  // optimize one-term case
  			Term term = termArray[0];
			int index = indexArray[0];
  			Query termQuery = new HydraTermQuery(term, index);
  			return termQuery.createWeight(searcher);
  		}
  		return new HydraPhraseWeight(searcher, termArray, indexArray);
  	}
}
