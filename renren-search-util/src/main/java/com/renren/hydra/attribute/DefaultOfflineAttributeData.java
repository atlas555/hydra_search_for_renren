package com.renren.hydra.attribute;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.renren.hydra.attribute.AttributeDataProcessorHolder.AttributeDataProcessor;
import com.renren.hydra.config.schema.Schema;

public class DefaultOfflineAttributeData extends OfflineAttributeData {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger
			.getLogger(DefaultOfflineAttributeData.class);

	protected Comparable[] attributeValues;

	public DefaultOfflineAttributeData() {
		int numAttributeValue = Schema.getInstance().getNumOfflineAttributeDataMeta();
		attributeValues = new Comparable[numAttributeValue];
		for(int i=0;i<numAttributeValue;++i)
			attributeValues[i]=null;
	}

	private void putAttribute(String key, Comparable value) {
		int index = Schema.getInstance().getOfflineAttributeIdByName(key);
		if(index!=-1)
			attributeValues[index]=value;
		else
			logger.error("attribute name not exists in OfflineAttributeMeta: "+key);
	}

	@Override
	public Comparable getAttributeValue(String key) {
		int index = Schema.getInstance().getOfflineAttributeIdByName(key);
		if(index!=-1)
			return attributeValues[index];
		else
			return null;
	}
	
	@Override
	public Comparable getAttributeValue(int index) {
		if(index!=-1)
			return attributeValues[index];
		else
			return null;
	}
	
	@Override
	public Map<String,Comparable> getAttributeDataMap(){
		AttributeDataMeta[] attributeDataMetas = Schema.getInstance().getOfflineAttributeDataMetas();
		int num = attributeDataMetas.length;
		if(num!=attributeValues.length)
			return null;
		if(num!=0){
			Map<String,Comparable> attributeDataMap = new HashMap<String,Comparable>(num*2);
			for(int i=0;i<num;++i){
				AttributeDataMeta meta = attributeDataMetas[i];
				String name = meta.getName();
				attributeDataMap.put(name, this.attributeValues[i]);
			}
			return attributeDataMap;
		}
		return null;
	}
	
	@Override
	public boolean init(JSONObject obj) {
		AttributeDataMeta[] attriDataMetaArray = Schema.getInstance().getOfflineAttributeDataMetas();
		AttributeDataProcessor attrDataProcessor = AttributeDataProcessorHolder.getDataProcessor();
		for (AttributeDataMeta attriDataMeta : attriDataMetaArray) {
			String name = attriDataMeta.getName();
			try {
				String valueStr = obj.getString(name);
				Comparable value = attrDataProcessor.getValue(attriDataMeta, valueStr);
				putAttribute(name,value);
			} catch (Exception e) {
				logger.error("JSONObject parsed error !", e);
				return false;
			}
		}
		return true;
	}

}
