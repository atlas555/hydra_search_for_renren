package com.renren.hydra.search.scorer;

import java.io.IOException;
import org.apache.lucene.index.Term;

class HydraReqOptSumScorer extends HydraScorer {
	private HydraScorer reqScorer;
	private HydraScorer optScorer;
	private TermMatchInfo[] matchInfos;
	private long matchFlags = 0;
	private int totalTermCount = 0;
	private int matchTermCount = 0;

	public HydraReqOptSumScorer(HydraScorer reqScorer, HydraScorer optScorer) {
		super(null); // No similarity used.
		this.reqScorer = reqScorer;
		this.optScorer = optScorer;
		
		totalTermCount = reqScorer.getTotalTermCount() + optScorer.getTotalTermCount();
		matchInfos = new TermMatchInfo[totalTermCount];
	}

	@Override
	public int nextDoc() throws IOException {
		int curDoc = reqScorer.nextDoc();
		if (optScorer != null) {
			if (optScorer.docID() < curDoc) {
			   	optScorer.advance(curDoc);
			}
		}

		return curDoc;
	}
  
	@Override
	public int advance(int target) throws IOException {
		int curDoc = reqScorer.advance(target);
		if (optScorer != null) {
			if (optScorer.docID() < curDoc) {
			   	optScorer.advance(curDoc);
			}
		}

		return curDoc;
	}
  
	@Override
	public int docID() {
		return reqScorer.docID();
	}
	
	@Override
	public TermMatchInfo[] getMatchInfos() {
		matchFlags = 0;
		matchTermCount = 0;
		TermMatchInfo[] subMatchInfos = reqScorer.getMatchInfos();
		int subMatchTermCount = reqScorer.getMatchTermCount();
		for (int i = 0; i < subMatchTermCount; i++) {
			Term term = subMatchInfos[i].term;
			int index = subMatchInfos[i].index;
			if ((matchFlags & (1 << index)) == 0) {
				matchInfos[matchTermCount] = subMatchInfos[i];
				matchTermCount++;
				matchFlags = matchFlags | (1 << index);
			}
		}

		if (optScorer != null) {
			if (optScorer.docID() == reqScorer.docID()) {
				subMatchInfos = optScorer.getMatchInfos();
				subMatchTermCount = optScorer.getMatchTermCount();
				for (int i = 0; i < subMatchTermCount; i++) {
					Term term = subMatchInfos[i].term;
					int index = subMatchInfos[i].index;
					if ((matchFlags & (1 << index)) == 0) {
						matchInfos[matchTermCount] = subMatchInfos[i];
						matchTermCount++;
						matchFlags = matchFlags | (1 << index);
					}
				}
			}
		}
		
		return matchInfos;
	}
  
	@Override
	public float score() throws IOException {
		return 0.0f;
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
	public long getMatchFlags() {
		return matchFlags;
	}
}

