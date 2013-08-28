package com.renren.hydra.attribute;

import java.util.Map;

import org.json.JSONObject;

import com.renren.hydra.attribute.OfflineAttributeData;

public class ShareOfflineAttributeData extends OfflineAttributeData {
	private static final long serialVersionUID = 1L;
	
    public boolean isAd;
    public long contentFingerPrint;
    public double qualityScore;
    public double heatScore;
	public long shareCount;
	public long viewCount;
	
	@Override
	public boolean init(JSONObject obj) {
		isAd = obj.optBoolean("isAd", false);
        contentFingerPrint = obj.optLong("contentFingerPrint", 0);
        qualityScore = obj.optDouble("qualityScore", 0.05);
        heatScore = obj.optDouble("heatScore", 0);
        shareCount = obj.optLong("share_count", 0);
        viewCount = obj.optLong("view_count", 0);
		return true;
	}

	@Override
	public Comparable getAttributeValue(String key) {
		return null;
	}

	@Override
	public Map<String, Comparable> getAttributeDataMap() {
		return null;
	}

	@Override
	public Comparable getAttributeValue(int index) {
		// TODO Auto-generated method stub
		return null;
	}

}
