package com.renren.hydra.search.parser;

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Explanation.IDFExplanation;
import org.apache.lucene.search.Similarity;

import com.renren.hydra.search.scorer.HydraTermScorer;

public class HydraTermQuery extends TermQuery {
	private static final long serialVersionUID = 1L;
	
	private int termIndex;

	public HydraTermQuery(Term t, int termIndex) {
		super(t);
		this.termIndex = termIndex;
	}

	public class HydraTermWeight extends Weight {
		private static final long serialVersionUID = 1L;
		
		private Similarity similarity;
		private Term term;
		private int termIndex;
		private float value;
		private float idf;
		private float queryNorm;
		private float queryWeight;
		private IDFExplanation idfExp;

		public HydraTermWeight(Searcher searcher, Term term, int termIndex) throws IOException {
			this.similarity = getSimilarity(searcher);
			this.term = term;
			this.termIndex = termIndex;
			idfExp = similarity.idfExplain(term, searcher);
			idf = idfExp.getIdf();
		}

		public float getIdf() {
			return idf;
		}

		public Term getTerm() {
			return term;
		}

		public int getTermIndex() {
			return termIndex;
		}

		public String toString() { return "weight(" + HydraTermQuery.this + ")"; }
		
		public Query getQuery() { return HydraTermQuery.this; }
		
		public float getValue() { return value; }

		public float sumOfSquaredWeights() {
			queryWeight = idf * getBoost();             // compute query weight
			return queryWeight * queryWeight;           // square it
		}

		public void normalize(float queryNorm) {
			this.queryNorm = queryNorm;
			queryWeight *= queryNorm;                   // normalize query weight
			value = queryWeight * idf;                  // idf for document
		}

		public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) 
			throws IOException {
			TermDocs termDocs = reader.termDocs(term);
			TermPositions termPositions = reader.termPositions(term);
			if (termDocs == null || termPositions == null)
				return null;
		
			return new HydraTermScorer(this, termDocs, termPositions, similarity);
		}

		public Explanation explain(IndexReader reader, int doc) throws IOException {
			return new Explanation();
		}
	}

	public Weight createWeight(Searcher searcher) throws IOException {
		return new HydraTermWeight(searcher, getTerm(), termIndex);
	}
}
