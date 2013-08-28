package com.renren.hydra.search.similarity;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.scorer.TermMatchInfo;

public abstract class HydraSimilarity {
	protected Query query;
	protected int partition;
	protected IndexReader indexReader;
	protected Map<String, String> userInfo;
    protected Map<Integer, Map<MutableInt, Short>> friendsInfo;
	
	protected Schema _schema;

	public boolean showExplain;

	public HydraSimilarity(Schema schema, Query query,boolean showExplain) {
		this.query = query;
		this._schema = schema;
		this.showExplain = showExplain;
	}
	
	public void setUserInfo(Map<String, String> userInfo) {
		this.userInfo = userInfo;
	}

	public void setFriendsInfo(Map<Integer, Map<MutableInt, Short>> friendsInfo) {
		this.friendsInfo = friendsInfo;
	}

	public void setPartitionId(int partition) {
		this.partition = partition;
	}


	public void setIndexReader(IndexReader indexReader) {
		this.indexReader = indexReader;
	}
	
	public abstract float score(int docid, int baseDocId, long uid,TermMatchInfo[] matchInfos,int matchTermCount, 
			OnlineAttributeData onlineAttributeData, OfflineAttributeData offlineAttributeData) throws Exception;
	public abstract Map<String, Object> getKVForExplain();
	public abstract String explain(Map<String, Object> kvForExplain);
}
