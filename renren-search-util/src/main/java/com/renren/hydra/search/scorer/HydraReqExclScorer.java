package com.renren.hydra.search.scorer;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;

class HydraReqExclScorer extends HydraScorer {
	private HydraScorer reqScorer;
	private DocIdSetIterator exclDisi;
	private int doc = -1;

	public HydraReqExclScorer(HydraScorer reqScorer, DocIdSetIterator exclDisi) {
		super(null); // No similarity used.
		this.reqScorer = reqScorer;
		this.exclDisi = exclDisi;
	}

	@Override
	public int nextDoc() throws IOException {
		if (reqScorer == null) {
			return doc;
		}
		doc = reqScorer.nextDoc();
		if (doc == NO_MORE_DOCS) {
			reqScorer = null; // exhausted, nothing left
			return doc;
		}
		if (exclDisi == null) {
			return doc;
		}
		return doc = toNonExcluded();
	}
  
	private int toNonExcluded() throws IOException {
		int exclDoc = exclDisi.docID();
		int reqDoc = reqScorer.docID(); // may be excluded
		do {  
			if (reqDoc < exclDoc) {
				return reqDoc; // reqScorer advanced to before exclScorer, ie. not excluded
			} else if (reqDoc > exclDoc) {
				exclDoc = exclDisi.advance(reqDoc);
				if (exclDoc == NO_MORE_DOCS) {
					exclDisi = null; // exhausted, no more exclusions
					return reqDoc;
				}
				if (exclDoc > reqDoc) {
					return reqDoc; // not excluded
				}
			}
		} while ((reqDoc = reqScorer.nextDoc()) != NO_MORE_DOCS);
		reqScorer = null; // exhausted, nothing left
		return NO_MORE_DOCS;
	}

	@Override
	public int docID() {
		return doc;
	}

	@Override
	public float score() throws IOException {
		return 0.0f;
	}
  
	@Override
	public int advance(int target) throws IOException {
		if (reqScorer == null) {
			return doc = NO_MORE_DOCS;
		}
		if (exclDisi == null) {
			return doc = reqScorer.advance(target);
		}
		if (reqScorer.advance(target) == NO_MORE_DOCS) {
			reqScorer = null;
			return doc = NO_MORE_DOCS;
		}
		return doc = toNonExcluded();
	}

	@Override
	public long getMatchFlags() {
		return reqScorer.getMatchFlags();
	}

	@Override
	public int getTotalTermCount() {
		return reqScorer.getTotalTermCount();
	}

	@Override
	public TermMatchInfo[] getMatchInfos() {
		return reqScorer.getMatchInfos();
	}

	@Override
	public int getMatchTermCount() {
		return reqScorer.getMatchTermCount();
	}
}
