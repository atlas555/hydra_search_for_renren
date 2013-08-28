package com.renren.hydra.searcher.core.search;


import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.HydraSortScoreDocPriorityQueue;
import com.renren.hydra.search.scorer.TermMatchInfo;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.search.sort.SortField;

public class HydraSortScoreDocCollector extends HydraCollector<HydraScoreDoc> {
	private static Logger logger = Logger.getLogger(HydraSortScoreDocCollector.class);
	
	private HydraScoreDoc tempScoreDoc;
	private SortField[] sortFields;
	private Schema schema;
	
	private int[] sortField2AttributeId;
	private SortField.DataSourceType[] sortFieldSourceTypes;
	
	public HydraSortScoreDocCollector(int numDoc,SortField[] sortFields) {
		this(numDoc,sortFields,false);
	}
	
	public HydraSortScoreDocCollector(int numDoc,SortField[] sortFields, boolean needScore) {
		super(new HydraSortScoreDocPriorityQueue<HydraScoreDoc>(numDoc,sortFields),needScore);
		this.sortFields = sortFields;
		this.needScore = needScore;
		if(this.sortFields==null)
			this.needScore = true;
		this.schema=Schema.getInstance();
		
		if(this.sortFields!=null){
			int len = sortFields.length;
			sortField2AttributeId = new int[len];
			sortFieldSourceTypes = new SortField.DataSourceType[len];
			String fieldName;
			for (int i = 0; i < len; ++i) {
				fieldName = sortFields[i].getField();
				SortField.DataSourceType dsType = sortFields[i].getSourceType();
				sortFieldSourceTypes[i] = dsType;
				if(dsType==SortField.DataSourceType.ONLINE)
					sortField2AttributeId[i] = schema.getOnlineAttributeIdByName(fieldName);
				else if(dsType==SortField.DataSourceType.OFFLINE)
					sortField2AttributeId[i] = schema.getOfflineAttributeIdByName(fieldName);
				else
					sortField2AttributeId[i] = -1;
			}
		}
	}
	
	public HydraSortScoreDocCollector(int numDoc, Sort sort){
		this(numDoc, sort==null?null:sort.getSort(),sort==null?true:sort.isNeedScore());
	}

	@Override
	public void collect(int docid, int baseDocId, long uid,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) throws Exception {
		Comparable[] fields = null;
		boolean allSortFieldExists = true;
		if(null!=this.sortFields){
			int num = this.sortFields.length;
			fields = new Comparable[num];
			for(int i=0;i<num;i++){
				fields[i] = getFieldValue(docid,baseDocId,uid,onlineAttributeData,offlineAttributeData,i);
				if(null==fields[i]){
					allSortFieldExists =false;
					break;
				}
			}
			
		}
		
		if(allSortFieldExists){
			HydraScoreDoc scoreDoc = null;
			float score = 0.0f;
			if(needScore){
				score = score(docid, baseDocId, uid,onlineAttributeData,offlineAttributeData);
			}
			if(Float.isNaN(score))
				score=0.0f;
			if(tempScoreDoc == null)
				scoreDoc = new HydraScoreDoc(docid + baseDocId,score);
			else
				scoreDoc = tempScoreDoc;
			scoreDoc.fields = fields;
			scoreDoc.doc = docid + baseDocId;
			scoreDoc.score = score;
			scoreDoc.explainationStr = similarity.showExplain?similarity.explain(similarity.getKVForExplain()):"";
			scoreDoc._partition = partition;
			scoreDoc._uid = uid;
			//scoreDoc.onlineAttributeData = onlineAttributeData;
			//scoreDoc.offlineAttributeData = offlineAttributeData;
			tempScoreDoc = pq.insertWithOverflow(scoreDoc);	
		}
	}
	
	
	public Comparable getFieldValue(int docid,int baseDocId,long uid,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData,int index){
		Comparable value = null;
		SortField.DataSourceType dsType = sortFieldSourceTypes[index];
		if(dsType==null)
			return null;
		switch(dsType){
			case INDEX:
				value=getIndexFieldValue(docid,sortFields[index]);
				break;
			case ONLINE:
				value = getOnlineFieldValue(onlineAttributeData, index);
				break;
			case OFFLINE:
				value = getOfflineFieldValue(offlineAttributeData, index);
				break;
			case RESERVE:
				value = getReserveFieldValue(docid,baseDocId,uid,onlineAttributeData,offlineAttributeData,sortFields[index]);
				break;
			default:
				break;
		}
		if(!sortFields[index].isTypeValid(value)){
			return null;
		}
		return value;
	}
	
	private Comparable getReserveFieldValue(int docid, int baseDocId, long uid,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData,SortField sortField) {
		Comparable value = null;
		SortField.DataType type = sortField.getType();
		if(type==null)
			return null;
		switch(type){
			case  DOC:
				value = docid+baseDocId;
				break;
			case UID:
				value = uid;
				break;
			case SCORE:{
				try{
					value = score(docid, baseDocId, uid,onlineAttributeData,offlineAttributeData);
				}catch(Exception e){
					logger.error(e);
					value = null;
				}
			}
				break;
			default:
				break;
		}
		return value;
	}

	public Comparable getIndexFieldValue(int docid,SortField field){
		try{
			Document doc = this.indexReader.document(docid);
			String value = doc.get(field.getField());
			return value;
		}catch (Exception e){
			logger.error(e);
			return null;
		}
	}
	
	public Comparable getOnlineFieldValue(OnlineAttributeData attr, int index) {
		if (attr == null)
			return null;
		return (Comparable) attr.getAttributeValue(sortField2AttributeId[index]);
	}
	
	public Comparable getOfflineFieldValue(OfflineAttributeData attr, int index) {
		if (attr == null)
			return schema.getDefaultOfflineAttributeValue(sortField2AttributeId[index]);
		return (Comparable) attr.getAttributeValue(sortField2AttributeId[index]);
	}

}
