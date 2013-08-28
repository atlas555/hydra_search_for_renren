package com.renren.hydra.search.scorer;

import java.io.IOException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Collector;

import com.renren.hydra.search.parser.HydraTermQuery.HydraTermWeight;

public class HydraTermScorer extends HydraScorer {
	private static final float[] SIM_NORM_DECODER = Similarity.getNormDecoder();
	
	private Weight weight;
	private TermDocs termDocs;
	private int doc = -1;
	private int termIndex;
	private final int[] docs = new int[32];         // buffered doc numbers
	private final int[] freqs = new int[32];        // buffered term freqs
	private int pointer;
	private int pointerMax;
	private TermMatchInfo[] matchInfos;
	private TermMatchInfo[] lastMatchInfos;
	private long matchFlags;

	private static final int SCORE_CACHE_SIZE = 32;
	private float[] scoreCache = new float[SCORE_CACHE_SIZE];

	public HydraTermScorer(HydraTermWeight weight, TermDocs td, TermPositions termPositions, Similarity similarity) {
		super(similarity);
		matchInfos = new TermMatchInfo[1];
		Term term = weight.getTerm();
		termIndex = weight.getTermIndex();
		matchFlags = 1 << termIndex;
		
		matchInfos[0] = new TermMatchInfo();
		matchInfos[0].term = term;
		matchInfos[0].idf = weight.getIdf();
		matchInfos[0].index = termIndex;
		matchInfos[0].positions = termPositions;

		lastMatchInfos = new TermMatchInfo[1];
		lastMatchInfos[0] = new TermMatchInfo();
		lastMatchInfos[0].term = term;
		lastMatchInfos[0].idf = matchInfos[0].idf;
		lastMatchInfos[0].index = termIndex;
		lastMatchInfos[0].positions = termPositions;

	    this.weight = weight;
	    termDocs = td;
	}
	  
	@Override
	public void score(Collector c) throws IOException {
	}

	@Override
	protected boolean score(Collector c, int end, int firstDocID) throws IOException {
		return true;
	}

	@Override
	public int docID() { return doc; }

	@Override
	public int nextDoc() throws IOException {
		pointer++;
		if (pointer >= pointerMax) {
			pointerMax = termDocs.read(docs, freqs);    // refill buffer
			if (pointerMax != 0) {
				pointer = 0;
			} else {
				termDocs.close();                         // close stream
				return doc = NO_MORE_DOCS;
			}
		} 
		doc = docs[pointer];
		matchInfos[0].tf = freqs[pointer];

		return doc;
	}
	  
	@Override
	public float score() {
		return (float)0.0;
	}

	@Override
	public int advance(int target) throws IOException {
		// first scan in cache
		for (pointer++; pointer < pointerMax; pointer++) {
			if (docs[pointer] >= target) {
				matchInfos[0].tf = freqs[pointer];
				return doc = docs[pointer];
			}
		}

		// not found in cache, seek underlying stream
		boolean result = termDocs.skipTo(target);
		if (result) {
			pointerMax = 1;
			pointer = 0;
			docs[pointer] = doc = termDocs.doc();
			freqs[pointer] = termDocs.freq();
		} else {
			doc = NO_MORE_DOCS;
			return doc;
		}
		
		matchInfos[0].tf = freqs[pointer];
		return doc;
	}
	  
	@Override
	public String toString() { return "scorer(" + weight + ")"; }

	@Override
	public TermMatchInfo[] getMatchInfos() {
		lastMatchInfos[0].tf = matchInfos[0].tf;		
		return lastMatchInfos;
	}

	@Override
	public long getMatchFlags() {
		return matchFlags;
	}

	@Override
	public int getTotalTermCount() {
		return 1;
	}

	@Override
	public int getMatchTermCount() {
		return 1;
	}
}
