package com.renren.hydra.search.filter;

import org.apache.log4j.Logger;

import com.renren.hydra.attribute.AttributeDataMeta;
import com.renren.hydra.attribute.AttributeDataProcessorHolder;
import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.attribute.AttributeDataProcessorHolder.AttributeDataProcessor;
import com.renren.hydra.config.schema.Schema;

public class AttributeSearchFilter extends SearchFilter {
	private static  Logger logger = Logger.getLogger(AttributeSearchFilter.class);
	
	public static class AttributeFilterCondition {
		public enum AttributeFilterOperator{
			EQ,NE,GT,GE,LT,LE
		}
		
		public  enum DataSourceType{
			NONE,ONLINE,OFFLINE
		}
		
		private String attributeName;
		private Comparable value;
		private AttributeFilterOperator operator;
		private DataSourceType dsType;
		private int index;
		
		public AttributeFilterCondition(String name,DataSourceType dsType,AttributeFilterOperator op,Comparable value){
			this.attributeName=name;
			this.dsType=dsType;
			this.operator=op;
			this.value=value;
			if(dsType==DataSourceType.ONLINE)
				this.index = Schema.getInstance().getOnlineAttributeIdByName(this.attributeName);
			else if(dsType==DataSourceType.OFFLINE)
				this.index=Schema.getInstance().getOfflineAttributeIdByName(this.attributeName);
			else
				this.index=-1;
		}
		
		public static AttributeFilterCondition getAttributeFilterCondition(String filterCondition){
			if(filterCondition==null||filterCondition.equals("")){
				logger.error("filterCondition error:"+filterCondition);
				return null;
			}
			
			String[] filterInfos = filterCondition.split(" ",3);
			if(filterInfos.length!=3){
				logger.error("filterCondition error, split size not equal 3:"+filterCondition);
				return null;
			}
			
			String attributeName=filterInfos[0];
			AttributeFilterOperator op=null;
			try{
				op=AttributeFilterOperator.valueOf(filterInfos[1].toUpperCase());
			}catch(IllegalArgumentException e){
				logger.error("filterCondition operator error:"+e);
				return null;
			}

			DataSourceType dsType = getDataSourceType(attributeName);
			if(dsType==DataSourceType.NONE){
				logger.error("filterCondition error, attribute name not in online/offline attribute:"+filterCondition);
				return null;
			}
			
			Comparable value = getValue(attributeName,filterInfos[2],dsType);
			if(value==null){
				logger.error("filterCondition error,filterValue error:"+filterCondition);
				return null;
			}
			return new AttributeFilterCondition(attributeName,dsType,op,value);
		}
		
		public static DataSourceType getDataSourceType(String fieldName){
			DataSourceType dsType = DataSourceType.NONE;
			Schema schema = Schema.getInstance();
			//优先选择OnlineAttributeData
			int index = schema.getOnlineAttributeIdByName(fieldName);
			if(index==-1){
				index=schema.getOfflineAttributeIdByName(fieldName);
				if(index!=-1)
					dsType=DataSourceType.OFFLINE;
			}else
				dsType=DataSourceType.ONLINE;

			return dsType;
		}
		
		public static Comparable getValue(String fieldName, String strValue,DataSourceType dsType){
			AttributeDataProcessor dataProcessor = AttributeDataProcessorHolder.getDataProcessor();
			Schema schema = Schema.getInstance();
			AttributeDataMeta attrDataMeta=null;
	
			if(dsType==DataSourceType.ONLINE){
				int index = schema.getOnlineAttributeIdByName(fieldName);
				attrDataMeta=schema.getOnlineAttributeDataMetas()[index];
			}else{
				int index=schema.getOfflineAttributeIdByName(fieldName);
				attrDataMeta=schema.getOfflineAttributeDataMetas()[index];
			}
			
			if(attrDataMeta==null){
				logger.error("no attribute meta for:"+fieldName);
				return null;
			}
			
			Comparable value=null;
			try {
				value = dataProcessor.getValue(attrDataMeta, strValue);
			} catch (Exception e) {
				logger.error("parser value failed:"+strValue);
				return null;
			}
			return value;
		}
	}
	
	private AttributeFilterCondition attrFilterCondition;
	
	public AttributeSearchFilter(String filterCondition) {
		super(filterCondition);
		this.attrFilterCondition=AttributeFilterCondition.getAttributeFilterCondition(filterCondition);
	}



	@Override
	public boolean filter(Long uid, OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) {
		//过滤条件有错误，都符合条件
		if(this.attrFilterCondition==null)
			return true;
		
		Schema schema = Schema.getInstance();
		AttributeFilterCondition.DataSourceType dsType = this.attrFilterCondition.dsType;
		Comparable value = null;
		if(dsType==AttributeFilterCondition.DataSourceType.ONLINE){
			if(onlineAttributeData!=null)
				value=onlineAttributeData.getAttributeValue(this.attrFilterCondition.index);
			else
				value=schema.getDefaultOnlineAttributeValue(this.attrFilterCondition.index);
		}else{
			if(offlineAttributeData!=null)
				value = offlineAttributeData.getAttributeValue(this.attrFilterCondition.index);
			else
				value=schema.getDefaultOfflineAttributeValue(this.attrFilterCondition.index);
				
		}
		//待比较的属性值为null，该文档不符合条件，返回false
		if(value==null)
			return false;
		
		AttributeFilterCondition.AttributeFilterOperator op = this.attrFilterCondition.operator;
		int ret = value.compareTo(this.attrFilterCondition.value);
		switch(op){
			case  EQ:
				return ret==0;
			case NE:
				return ret!=0;
			case GT:
				return ret>0;
			case GE:
				return ret>=0;
			case LT:
				return ret<0;
			case LE:
				return ret<=0;
			default:
				return false;
		}
	}
}
