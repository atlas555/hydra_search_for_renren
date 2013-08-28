package com.renren.hydra.search.scorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Collector;

public class HydraBooleanScorer extends HydraScorer {
  
	private final List<HydraScorer> requiredScorers;
	private final List<HydraScorer> optionalScorers;
	private final List<HydraScorer> prohibitedScorers;

	private final HydraScorer countingSumScorer;
	private final int minNrShouldMatch;
	private int doc = -1;

	public HydraBooleanScorer(Similarity similarity, int minNrShouldMatch,
			List<HydraScorer> required, List<HydraScorer> prohibited, 
			List<HydraScorer> optional) 
					throws IOException {
		super(similarity);
		if (minNrShouldMatch < 0) {
			throw new IllegalArgumentException("Minimum number of optional scorers should not be negative");
		}
		this.minNrShouldMatch = minNrShouldMatch;

		optionalScorers = optional;
		requiredScorers = required;
		prohibitedScorers = prohibited;
		countingSumScorer = makeCountingSumScorer();
	}
  
	private class HydraSingleMatchScorer extends HydraScorer {
		private HydraScorer scorer;
		private int lastScoredDoc = -1;

		private float lastDocScore = Float.NaN;

		public HydraSingleMatchScorer(HydraScorer scorer) {
			super(scorer.getSimilarity());
			this.scorer = scorer;
		}

		@Override
		public float score() throws IOException {
			return 0.0f;
		}

		@Override
		public int docID() {
			return scorer.docID();
		}

		@Override
		public int nextDoc() throws IOException {
			return scorer.nextDoc();
		}

		@Override
		public int advance(int target) throws IOException {
			return scorer.advance(target);
		}
		
		@Override
		public TermMatchInfo[] getMatchInfos() {
			return scorer.getMatchInfos();
		}

		@Override
		public long getMatchFlags() {
			return scorer.getMatchFlags();
		}

		@Override
		public int getTotalTermCount() {
			return scorer.getTotalTermCount();
		}

		@Override
		public int getMatchTermCount() {
			return scorer.getMatchTermCount();
		}
	}

	private HydraScorer countingDisjunctionSumScorer(final List<HydraScorer> scorers,
			int minNrShouldMatch) throws IOException {
		return new HydraDisjunctionSumScorer(scorers, minNrShouldMatch);
	}

	private static final Similarity defaultSimilarity = Similarity.getDefault();

	private HydraScorer countingConjunctionSumScorer(List<HydraScorer> requiredScorers) 
			throws IOException {
		return new HydraConjunctionScorer(defaultSimilarity, requiredScorers);
	}

	private HydraScorer dualConjunctionSumScorer(HydraScorer req1, HydraScorer req2) 
			throws IOException {
		return new HydraConjunctionScorer(defaultSimilarity, new HydraScorer[]{req1, req2});
	}

	private HydraScorer makeCountingSumScorer() throws IOException {
		return (requiredScorers.size() == 0)
          ? makeCountingSumScorerNoReq()
          : makeCountingSumScorerSomeReq();
	}

	private HydraScorer makeCountingSumScorerNoReq() throws IOException {
		int nrOptRequired = (minNrShouldMatch < 1) ? 1 : minNrShouldMatch;
		HydraScorer requiredCountingSumScorer;
		if (optionalScorers.size() > nrOptRequired) {
			requiredCountingSumScorer = countingDisjunctionSumScorer(optionalScorers, nrOptRequired);
		} else if (optionalScorers.size() == 1)
			requiredCountingSumScorer = new HydraSingleMatchScorer(optionalScorers.get(0));
		else
			requiredCountingSumScorer = countingConjunctionSumScorer(optionalScorers);
		return addProhibitedScorers(requiredCountingSumScorer);
	}

	private HydraScorer makeCountingSumScorerSomeReq() throws IOException {
		if (optionalScorers.size() == minNrShouldMatch) { // all optional scorers also required.
			ArrayList<HydraScorer> allReq = new ArrayList<HydraScorer>(requiredScorers);
			allReq.addAll(optionalScorers);
			return addProhibitedScorers(countingConjunctionSumScorer(allReq));
		} else { // optionalScorers.size() > minNrShouldMatch, and at least one required scorer
			HydraScorer requiredCountingSumScorer = requiredScorers.size() == 1
					? new HydraSingleMatchScorer(requiredScorers.get(0)) : 
					countingConjunctionSumScorer(requiredScorers);
			if (minNrShouldMatch > 0) { // use a required disjunction scorer over the optional scorers
				return addProhibitedScorers( 
						dualConjunctionSumScorer(requiredCountingSumScorer,
                              countingDisjunctionSumScorer(optionalScorers, minNrShouldMatch)));
			} else { // minNrShouldMatch == 0
				return new HydraReqOptSumScorer(
                      addProhibitedScorers(requiredCountingSumScorer), optionalScorers.size() == 1
                        ? new HydraSingleMatchScorer(optionalScorers.get(0)) : 
						countingDisjunctionSumScorer(optionalScorers, 1));
			}
		}
	}
  
	private HydraScorer addProhibitedScorers(HydraScorer requiredCountingSumScorer) 
		throws IOException 
	{
		return (prohibitedScorers.size() == 0)
          ? requiredCountingSumScorer // no prohibited
          : new HydraReqExclScorer(requiredCountingSumScorer,
                              ((prohibitedScorers.size() == 1)
                                ? prohibitedScorers.get(0)
                                : new HydraDisjunctionSumScorer(prohibitedScorers)));
	}

	@Override
	public void score(Collector collector) throws IOException {}
  
	@Override
	protected boolean score(Collector collector, int max, int firstDocID) throws IOException {
		return false;
	}

	@Override
	public int docID() {
		return doc;
	}
  
	@Override
	public int nextDoc() throws IOException {
		return doc = countingSumScorer.nextDoc();
	}
  
	@Override
	public float score() throws IOException {
		return 0.0f;
	}

	@Override
	public int advance(int target) throws IOException {
		return doc = countingSumScorer.advance(target);
	}
	
	@Override
	public TermMatchInfo[] getMatchInfos() {
		return countingSumScorer.getMatchInfos();
	}

	@Override
	public long getMatchFlags() {
		return countingSumScorer.getMatchFlags();
	}

	@Override
	public int getTotalTermCount() {
		return countingSumScorer.getTotalTermCount();
	}

	@Override
	public int getMatchTermCount() {
		return countingSumScorer.getMatchTermCount();
	}
}


