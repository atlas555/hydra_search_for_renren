package com.renren.hydra.index;


import org.apache.lucene.analysis.Analyzer;
import org.json.JSONObject;

import com.renren.hydra.attribute.AttributeDataMeta;
import com.renren.hydra.attribute.AttributeDataProcessorHolder;
import com.renren.hydra.attribute.AttributeDataProcessorHolder.AttributeDataProcessor;
import com.renren.hydra.attribute.DefaultOnlineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;

public class DefaultDocumentProcessor extends DocumentProcessor {

	private AttributeDataMeta[] attriDataMetaArray;
	public DefaultDocumentProcessor(Analyzer analyzer, Schema schema) {
		super(analyzer, schema);
		attriDataMetaArray = _schema.getOnlineAttributeDataMetas();
	}

	@Override
	public OnlineAttributeData createAttributeData() {
		DefaultOnlineAttributeData attrData = new DefaultOnlineAttributeData(this._schema);
		return attrData;
	}

	@Override
	public void processAttribute(JSONObject obj, OnlineAttributeData attrData)
			throws Exception {
		AttributeDataProcessor attrDataProcessor=AttributeDataProcessorHolder.getDataProcessor();
		for (AttributeDataMeta attriDataMeta : attriDataMetaArray) {
			String name = attriDataMeta.getName();
			AttributeDataMeta.DataType type = attriDataMeta.getType();
			if(obj.has(name)){
				String valueStr = obj.optString(name);
				Comparable value = attrDataProcessor.getValue(attriDataMeta, valueStr);
				attrData.putAttribute(name, value);
			}
		}
	}

	@Override
	public long getUid(JSONObject obj) {
		return obj.optLong(_schema.getUidField());
	}

	@Override
	public void preProcess(JSONObject obj) {
		
	}

}
