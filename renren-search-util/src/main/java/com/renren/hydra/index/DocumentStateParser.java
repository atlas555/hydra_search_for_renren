package com.renren.hydra.index;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import com.renren.hydra.attribute.OnlineAttributeManager;


public class DocumentStateParser {
	protected static Logger logger = Logger.getLogger(DocumentStateParser.class);
	
	public static final String UPDATE_ATTRIBUTE_ONLY = "_upAttOnly";
	public static final String DELETE_FIELD="_delete";
	public static final String SKIP_FIELD="_skip";
	public static enum DocState { DELETE, UPDATE, UPDATEATTRONLY, ADD, SKIP,NONE}
	
	/*
	 * get all Document deal state
	 *  
	 * */
	public static DocState getDocState(JSONObject obj,OnlineAttributeManager _onlineAttrManager,long key) {
		boolean deleteFlag = isDeleted(obj);
		if(deleteFlag){
			return DocState.DELETE;
		}else{
			if(_onlineAttrManager.contains(key)) {
				boolean updateOnlyFlag = isUpdateAttributeOnly(obj);
				if(updateOnlyFlag) {
					return DocState.UPDATEATTRONLY;
				}else
					return DocState.UPDATE;
			} else {
				return DocState.ADD;
			}
		}
	}
	
	public static boolean getBooleanValue(JSONObject obj, String field,boolean defaultValue){
		boolean ret = defaultValue;
		if(!obj.has(field))
			return ret;
		ret = obj.optBoolean(field, defaultValue);
		return ret;
	}
	
	public static boolean isSkip(JSONObject obj){
		return getBooleanValue(obj,SKIP_FIELD,false);
	}
	
	public static boolean isDeleted(JSONObject obj){
		return getBooleanValue(obj,DELETE_FIELD,false);
	}
	
	
	private static boolean isUpdateAttributeOnly(JSONObject obj) {
		return getBooleanValue(obj,UPDATE_ATTRIBUTE_ONLY,false);
	}
}

