package com.renren.hydra.attribute;

import java.io.Serializable;
import java.util.Map;

import org.json.JSONObject;

public abstract class OfflineAttributeData implements Serializable {
	private static final long serialVersionUID = 1L;

	public abstract boolean init(JSONObject obj);
	public abstract Comparable getAttributeValue(int index);
	public abstract Comparable getAttributeValue(String key);
	public abstract Map<String,Comparable> getAttributeDataMap();
}
