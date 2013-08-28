package com.renren.hydra.search.scorer;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.index.Term;

public class HydraConjunctionScorer extends HydraScorer {
  
	private final HydraScorer[] scorers;
	private int lastDoc = -1;
	private TermMatchInfo[] matchInfos;
	private long matchFlags = 0;
	private int totalTermCount = 0;
	private int matchTermCount = 0;

	public HydraConjunctionScorer(Similarity similarity, Collection<HydraScorer> scorers) 
		throws IOException 
	{
		this(similarity, scorers.toArray(new HydraScorer[scorers.size()]));
	}

	public HydraConjunctionScorer(Similarity similarity, HydraScorer... scorers) throws IOException {
		super(similarity);
		this.scorers = scorers;
		matchFlags = 0;
		
		for (int i = 0; i < scorers.length; i++) {
			totalTermCount += scorers[i].getTotalTermCount();
			if (scorers[i].nextDoc() == NO_MORE_DOCS) {
				lastDoc = NO_MORE_DOCS;
				return;
			}
		}

		Arrays.sort(scorers, new Comparator<HydraScorer>() {         // sort the array
								public int compare(HydraScorer o1, HydraScorer o2) {
									return o1.docID() - o2.docID();
								}}
		);

		if (doNext() == NO_MORE_DOCS) {
			lastDoc = NO_MORE_DOCS;
			return;
		}

		matchInfos = new TermMatchInfo[totalTermCount];
		int end = scorers.length - 1;
		int max = end >> 1;
		for (int i = 0; i < max; i++) {
			HydraScorer tmp = scorers[i];
			int idx = end - i - 1;
			scorers[i] = scorers[idx];
			scorers[idx] = tmp;
		}
	}

	private int doNext() throws IOException {
		int first = 0;
		int doc = scorers[scorers.length - 1].docID();
		HydraScorer firstScorer;
		while ((firstScorer = scorers[first]).docID() < doc) {
			doc = firstScorer.advance(doc);
			first = first == scorers.length - 1 ? 0 : first + 1;
		}
		return doc;
	}
  
  	@Override
  	public int advance(int target) throws IOException {
  		if (lastDoc == NO_MORE_DOCS) {
  			return lastDoc;
  		} else if (scorers[(scorers.length - 1)].docID() < target) {
  			scorers[(scorers.length - 1)].advance(target);
  		}
  		return lastDoc = doNext();
  	}

  	@Override
  	public int docID() {
  		return lastDoc;
  	}
  
  	@Override
  	public int nextDoc() throws IOException {
  		if (lastDoc == NO_MORE_DOCS) {
  			return lastDoc;
  		} else if (lastDoc == -1) {
  			return lastDoc = scorers[scorers.length - 1].docID();
  		}
  		scorers[(scorers.length - 1)].nextDoc();
  		return lastDoc = doNext();
  	}
  
  	@Override
  	public float score() throws IOException {
  		return 0.0f; 
  	}
  	
	@Override
	public TermMatchInfo[] getMatchInfos() {
		matchFlags = 0;
		matchTermCount = 0;
		for (int i = 0; i < scorers.length; i++) {
			TermMatchInfo[] subMatchInfos = scorers[i].getMatchInfos();
			int subMatchTermCount = scorers[i].getMatchTermCount();
			for (int j = 0; j < subMatchTermCount; j++) {
				Term term = subMatchInfos[j].term;
				int index = subMatchInfos[j].index; 
				if ((matchFlags & (1 << index)) == 0) {
					matchInfos[matchTermCount] = subMatchInfos[j];
					matchTermCount++;
					matchFlags = matchFlags | (1 << index);
				}
			}
		}
		
		return matchInfos;
	}

	@Override
	public long getMatchFlags() {
		return matchFlags;
	}

	@Override
	public int getTotalTermCount() {
		return totalTermCount;
	}

	@Override
	public int getMatchTermCount() {
		return matchTermCount;
	}
}
