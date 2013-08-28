package com.renren.hydra.searcher.core.search;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.PriorityQueue;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.HydraScorer;
import com.renren.hydra.search.scorer.TermMatchInfo;
import com.renren.hydra.search.similarity.HydraSimilarity;

public abstract class HydraCollector<T extends HydraScoreDoc> {
	private static Logger logger = Logger.getLogger(HydraCollector.class);
	
	protected IndexReader indexReader;
	protected PriorityQueue<T> pq;
	protected HydraScorer scorer;
	protected boolean needExplain;
	protected HydraSimilarity similarity;
	protected int partition;
	protected float maxScore = Float.NaN;
	protected boolean needScore;
	
	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public HydraSimilarity getSimilarity() {
		return similarity;
	}

	public void setSimilarity(HydraSimilarity similarity) {
		this.similarity = similarity;
	}
	
	public boolean isNeedExplain() {
		return needExplain;
	}
	public void setNeedExplain(boolean needExplain) {
		this.needExplain = needExplain;
	}
	public HydraScorer getScorer() {
		return scorer;
	}
	public void setScorer(HydraScorer scorer) {
		this.scorer = scorer;
	}
	
	public IndexReader getIndexReader() {
		return indexReader;
	}
	public void setIndexReader(IndexReader indexReader) {
		this.indexReader = indexReader;
	}
	
	public boolean isNeedScore() {
		return needScore;
	}

	public void setNeedScore(boolean needScore) {
		this.needScore = needScore;
	}
	
	public HydraCollector(PriorityQueue<T> pq){
		this(pq,true);
	}
	
	public HydraCollector(PriorityQueue<T> pq,boolean needScore){
		this.pq = pq;
		this.needScore = needScore;
	}
	
	public abstract void collect(int docid, int baseDocId, long uid,
			OnlineAttributeData onlineAttributeData, OfflineAttributeData offlineAttributeData) throws Exception;
	
	public ScoreDoc[] topDocs(){
		logger.debug("start to construct top docs");
		int hitCount = pq.size();
		ScoreDoc[] scoreDocs = null;
		int index = hitCount - 1;
		if (index >= 0) {
			scoreDocs = new ScoreDoc[hitCount];
			while (pq.size() > 0) {
				HydraScoreDoc scoreDoc = pq.pop();
				scoreDocs[index] = scoreDoc;
				index--;
			}
			maxScore = scoreDocs[0].score;
		}
		else
			scoreDocs = new ScoreDoc[0];
		return scoreDocs;
	}
	
	public float maxScore(){
		return this.maxScore;
	}
	
	public float score(int docid, int baseDocId, long uid,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) throws Exception{
		float score = 0.0f;
		if(!this.needScore)
			return score;
		TermMatchInfo[] matchInfos = scorer.getMatchInfos();
		int matchTermCount = scorer.getMatchTermCount();
		score= similarity.score(docid,baseDocId,uid,matchInfos, matchTermCount, onlineAttributeData,offlineAttributeData);
		return score;
	}
	
	
}
