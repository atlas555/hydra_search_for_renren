package com.renren.hydra.attribute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.renren.hydra.config.schema.Schema;

public abstract class OnlineAttributeData implements Serializable, Writable {
	private static final long serialVersionUID = 1L;
	protected int[] fieldTTF;
	protected int userId;

	public OnlineAttributeData() {
		int numIndexField = Schema.getInstance().getNumIndexField();
		fieldTTF = new int[numIndexField];
		for(int i=0;i<numIndexField;++i)
			fieldTTF[i]=0;

		userId = 0;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public int getUserId() {
		return userId;
	}

	public void setTTF(String fieldName, int ttf) {
		int index = Schema.getInstance().getIndexIdByFieldName(fieldName);
		if(0<=index && index<fieldTTF.length)
			fieldTTF[index] = ttf;
			
	}

	public int getTTF(int index){
		if(0<=index && index<fieldTTF.length)
			return fieldTTF[index];
		else
			return 0;
	}

	public void write(DataOutput out) throws IOException{
		out.writeInt(this.userId);
		int numIndexField = fieldTTF.length;
		for(int i=0;i<numIndexField;++i)
			out.writeInt(fieldTTF[i]);
	}

	public void readFields(DataInput in) throws IOException {
		this.userId = in.readInt();
		int numIndexField = fieldTTF.length;
		for (int i = 0; i < numIndexField; ++i) {
			int ttf = in.readInt();
			fieldTTF[i]=ttf;
		}
	}
	public abstract Comparable getAttributeValue(String key);
	public abstract Comparable getAttributeValue(int index);
	public abstract void putAttribute(String key, Comparable value);
	public abstract Map<String,Comparable> getAttributeDataMap();
}
