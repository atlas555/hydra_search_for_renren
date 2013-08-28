package com.renren.hydra.search.scorer;

import java.util.List;
import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.util.ScorerDocQueue;

class HydraDisjunctionSumScorer extends HydraScorer {
	private final int nrScorers;
	private final int minimumNrMatchers;
	private ScorerDocQueue scorerDocQueue;
	private int currentDoc = -1;
	protected int nrMatchers = -1;
	private TermMatchInfo[] matchInfos;
	private long matchFlags = 0;
	private int matchTermCount = 0;
	private int totalTermCount = 0;

	public HydraDisjunctionSumScorer(List<HydraScorer> subScorers, int minimumNrMatchers) 
		throws IOException {
		super(null);

		matchFlags = 0;	
		nrScorers = subScorers.size();

		if (minimumNrMatchers <= 0) {
			throw new IllegalArgumentException("Minimum nr of matchers must be positive");
		}
		if (nrScorers <= 1) {
			throw new IllegalArgumentException("There must be at least 2 subScorers");
		}

		this.minimumNrMatchers = minimumNrMatchers;

		initScorerDocQueue(subScorers);
	}
  
	public HydraDisjunctionSumScorer(List<HydraScorer> subScorers) throws IOException {
		this(subScorers, 1);
	}

	private void initScorerDocQueue(List<HydraScorer> subScorers) throws IOException {
		scorerDocQueue = new ScorerDocQueue(nrScorers);
		for (HydraScorer se : subScorers) {
			if (se.nextDoc() != NO_MORE_DOCS) {
				scorerDocQueue.insert(se);
			}
			totalTermCount += se.getTotalTermCount();
		}

		matchInfos = new TermMatchInfo[totalTermCount];
	}
	
	@Override
	public void score(Collector collector) throws IOException {}

	@Override
	protected boolean score(Collector collector, int max, int firstDocID) throws IOException {
		return false;
	}

	@Override
	public int nextDoc() throws IOException {
		if (scorerDocQueue.size() < minimumNrMatchers || !advanceAfterCurrent()) {
			currentDoc = NO_MORE_DOCS;
		}
		return currentDoc;
	}

	protected boolean advanceAfterCurrent() throws IOException {
		do { // repeat until minimum nr of matchers
			matchFlags = 0;
			currentDoc = scorerDocQueue.topDoc();
			HydraScorer scorer = null;
			nrMatchers = 1;
			matchTermCount = 0;
			do { // Until all subscorers are after currentDoc
				scorer = (HydraScorer)scorerDocQueue.top();			
				TermMatchInfo[] subMatchInfos = scorer.getMatchInfos();
				int subMatchTermCount = scorer.getMatchTermCount();
				for (int i = 0; i < subMatchTermCount; i++) {
					Term term = subMatchInfos[i].term;
					int index = subMatchInfos[i].index;
					if ((matchFlags & (1 << index)) == 0) {
						matchInfos[matchTermCount] = subMatchInfos[i];
						matchTermCount++;
						matchFlags = matchFlags | (1 << index);
					}
				}
				if (!scorerDocQueue.topNextAndAdjustElsePop()) {
					if (scorerDocQueue.size() == 0) {
						break; // nothing more to advance, check for last match.
					}
				}
				if (scorerDocQueue.topDoc() != currentDoc) {
					break; // All remaining subscorers are after currentDoc.
				}
				nrMatchers++;
			} while (true);
     
			if (nrMatchers >= minimumNrMatchers) {
				return true;
			} else if (scorerDocQueue.size() < minimumNrMatchers) {
				return false;
			}
		} while (true);
	}
  

	@Override
	public float score() throws IOException { return 0.0f; }
   
	@Override
	public int docID() {
		return currentDoc;
	}

	public int nrMatchers() {
		return nrMatchers;
	}

	@Override
	public long getMatchFlags() {
		return matchFlags;
	}

	@Override
	public TermMatchInfo[] getMatchInfos() {
		return matchInfos;
	}

	@Override
	public int getTotalTermCount() {
		return totalTermCount;
	}

	@Override
	public int getMatchTermCount() {
		return matchTermCount;
	}

	@Override
	public int advance(int target) throws IOException {
		if (scorerDocQueue.size() < minimumNrMatchers) {
			return currentDoc = NO_MORE_DOCS;
		}
		if (target <= currentDoc) {
			return currentDoc;
		}
		do {
			if (scorerDocQueue.topDoc() >= target) {
				return advanceAfterCurrent() ? currentDoc : (currentDoc = NO_MORE_DOCS);
			} else if (!scorerDocQueue.topSkipToAndAdjustElsePop(target)) {
				if (scorerDocQueue.size() < minimumNrMatchers) {
					return currentDoc = NO_MORE_DOCS;
				}
			}
		} while (true);
	}
}
