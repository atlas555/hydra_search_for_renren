package com.renren.hydra.searcher.core.search;

import org.apache.log4j.Logger;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.OneSortFieldScoreDocPriorityQueue.SearcherReverseScoreDocPriorityQueue;
import com.renren.hydra.search.scorer.OneSortFieldScoreDocPriorityQueue.SearcherScoreDocPriorityQueue;
import com.renren.hydra.search.sort.SortField;
import com.renren.hydra.search.sort.SortField.DataSourceType;

public class OneSortFieldHydraScoreDocCollector extends HydraCollector<HydraScoreDoc>{
	private static Logger logger = Logger.getLogger(OneSortFieldHydraScoreDocCollector.class);
	
	private SortField sortField;
	private HydraScoreDoc tempScoreDoc;
	private SortField.DataSourceType dsType;
	private int index;
	private Schema schema;
	
	public OneSortFieldHydraScoreDocCollector(int numDoc, SortField sortField,boolean needScore) {
		super(new SearcherScoreDocPriorityQueue<HydraScoreDoc>(numDoc,sortField),needScore);
		String fieldName = sortField.getField();
		this.sortField = sortField;
		this.dsType = sortField.getSourceType();
		schema = Schema.getInstance();
		if(dsType==SortField.DataSourceType.ONLINE)
			index = schema.getOnlineAttributeIdByName(fieldName);
		else if(dsType==SortField.DataSourceType.OFFLINE)
			index = schema.getOfflineAttributeIdByName(fieldName);
		else
			index = -1;
	}
	
	public OneSortFieldHydraScoreDocCollector(int numDoc, SortField sortField, boolean needScore,boolean reverse) {
		super(new SearcherReverseScoreDocPriorityQueue<HydraScoreDoc>(numDoc,sortField),needScore);
		String fieldName = sortField.getField();
		this.sortField = sortField;
		this.dsType = sortField.getSourceType();
		schema = Schema.getInstance();
		if(dsType==SortField.DataSourceType.ONLINE)
			index = schema.getOnlineAttributeIdByName(fieldName);
		else if(dsType==SortField.DataSourceType.OFFLINE)
			index = schema.getOfflineAttributeIdByName(fieldName);
		else
			index = -1;
	}
	

	@Override
	public void collect(int docid, int baseDocId, long uid,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) throws Exception {
		Comparable field =null;
		if(dsType==DataSourceType.ONLINE)
			field = getOnlineFieldValue(onlineAttributeData);
		else if(dsType==DataSourceType.OFFLINE)
			field = getOfflineFieldValue(offlineAttributeData);
		if(null!=field){
			HydraScoreDoc scoreDoc = null;
			float score = 0.0f;
			if(needScore){
				score = score(docid, baseDocId, uid,onlineAttributeData,offlineAttributeData);
			}
			if(Float.isNaN(score))
				score = 0.0f;
			if(tempScoreDoc == null)
				scoreDoc = new HydraScoreDoc(docid + baseDocId,score);
			else
				scoreDoc = tempScoreDoc;
			scoreDoc.fields = new Comparable[]{field};
			scoreDoc.doc = docid + baseDocId;
			scoreDoc.score = score;
			scoreDoc.explainationStr = similarity.showExplain?similarity.explain(similarity.getKVForExplain()):"";
			scoreDoc._partition = partition;
			scoreDoc._uid = uid;
			tempScoreDoc = pq.insertWithOverflow(scoreDoc);	
		}
		
	}
	
	public Comparable getOnlineFieldValue(OnlineAttributeData attr) {
		if (attr == null)
			return null;
		return (Comparable) attr.getAttributeValue(index);
	}
	
	public Comparable getOfflineFieldValue(OfflineAttributeData attr) {
		if (attr == null)
			return schema.getDefaultOfflineAttributeValue(index);
		return (Comparable) attr.getAttributeValue(index);
	}

}
