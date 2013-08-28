package com.renren.hydra.similarity;

import com.renren.hydra.search.scorer.HydraScoreDoc;

public class HydraShareScoreDoc extends HydraScoreDoc {
	/**
 * 
 */
private static final long serialVersionUID = 1L;

	public long firstCreateTime = 0L;
	public long lastCreateTime = 0L;
	public long viewCount = 0L;
	public long shareCount = 0L;
	
	public HydraShareScoreDoc(int doc, float score) {
		super(doc, score);
	}
}
