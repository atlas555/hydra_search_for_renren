package com.renren.hydra.searcher.core.search;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.HydraScoreDocPriorityQueue.SearcherScoreDocPriorityQueue;

public class HydraScoreDocCollector extends HydraCollector<HydraScoreDoc>{
	private HydraScoreDoc tempScoreDoc;
	
	public HydraScoreDocCollector(int numDoc) {
		super(new SearcherScoreDocPriorityQueue<HydraScoreDoc>(numDoc),true);
	}
	
	public HydraScoreDocCollector(int numDoc,boolean needScore) {
		super(new SearcherScoreDocPriorityQueue<HydraScoreDoc>(numDoc),needScore);
	}

	@Override
	public void collect(int docid, int baseDocId, long uid,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) throws Exception {
		HydraScoreDoc scoreDoc = null;
		float score = score(docid, baseDocId, uid,onlineAttributeData,offlineAttributeData);
		if(Float.isNaN(score))
			score = 0.0f;
		if(tempScoreDoc == null)
			scoreDoc = new HydraScoreDoc(docid + baseDocId,score);
		else
			scoreDoc = tempScoreDoc;
		scoreDoc.doc = docid + baseDocId;
		scoreDoc.score = score;
		scoreDoc.explainationStr = similarity.showExplain?similarity.explain(similarity.getKVForExplain()):"";
		scoreDoc._partition = partition;
		scoreDoc._uid = uid;
		tempScoreDoc = pq.insertWithOverflow(scoreDoc);	
		
	}

}
