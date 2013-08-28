package com.renren.hydra.search.scorer;

import java.util.Map;

import org.apache.lucene.search.ScoreDoc;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;

public class HydraScoreDoc extends ScoreDoc implements IDocScoreAble{
	private static final long serialVersionUID = 1L;
	
	public long _uid = Long.MIN_VALUE;
	public int _partition = -1;
	private Map<String,Comparable> onlineAttributeData;
	private Map<String,Comparable> offlineAttributeData;
	
	public Comparable[] fields;
	public String explainationStr;
	
	public HydraScoreDoc(int doc, float score) {
		this(doc,score,null);
	}

	public HydraScoreDoc(int doc, float score,Comparable[] fields) {
		super(doc, score);
		_uid = Long.MIN_VALUE;
		_partition = -1;
		this.fields = fields;
		this.onlineAttributeData = null;
		this.offlineAttributeData = null;
		this.explainationStr="";
	}
	
	@Override
	public long getUID() {
		return this._uid;
	}

	@Override
	public float getScore() {
		return this.score;
	}
	
	public void setExplaination(String explaination){
		this.explainationStr = explaination;
	}
	
	public String getExplaination(){
		return this.explainationStr;
	}
	
	public Comparable getOnlineAttribute(String attrName){
		if(this.onlineAttributeData==null)
			return null;
		return this.onlineAttributeData.get(attrName);
	}
	
	public Comparable getOfflineAttribute(String attrName){
		if(this.offlineAttributeData==null)
			return null;
		else
			return this.offlineAttributeData.get(attrName);
	}
	
	public int getDocId(){
		return this.doc;
	}
	
	public int getPartition(){
		return this._partition;
	}
	
	public void setOnlineAttributeData(Map<String,Comparable> attributeData){
		this.onlineAttributeData = attributeData;
	}
	
	public void setOfflineAttributeData(Map<String,Comparable> attributeData){
		this.offlineAttributeData = attributeData;
	}

	@Override
	public Comparable getFieldValue(int index) {
		if(this.fields==null)
			return null;
		int num = this.fields.length;
		if(0>index || num<=index){
			return null;
		}	
		return this.fields[index];
	}
	
	@Override
	public String toString() {
	    StringBuilder sb = new StringBuilder(super.toString());
	    sb.append("[");
	    for (int i = 0; i < fields.length; i++) {
	            sb.append(fields[i]).append(", ");
	          }
	    sb.setLength(sb.length() - 2); 
	    sb.append("]");
	    sb.append(" ");
	    sb.append("partition: ");
		sb.append(this._partition);
		sb.append(" uid: ");
		sb.append(this._uid);
		sb.append(super.toString());
	    return sb.toString();
	 }
		
}
