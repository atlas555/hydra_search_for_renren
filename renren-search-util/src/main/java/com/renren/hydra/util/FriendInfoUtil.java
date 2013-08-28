package com.renren.hydra.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

public class FriendInfoUtil {
	public static Map<Integer,Map<MutableInt,Short>> convert2Mutable(Map<Integer,Map<Integer,Short>> friendInfos){
		if(friendInfos==null || friendInfos.isEmpty())
			return null;
		Map<Integer,Map<MutableInt,Short>> retMap = new HashMap<Integer,Map<MutableInt,Short>>();
		
		for(Map.Entry<Integer, Map<Integer,Short>> entry : friendInfos.entrySet()){
			Integer key = entry.getKey();
			Map<Integer,Short> value = entry.getValue();
			Map<MutableInt,Short> newValue = new HashMap<MutableInt,Short>();
			if(value!=null && !value.isEmpty()){
				for(Map.Entry<Integer, Short> infoEntry : value.entrySet()){
					newValue.put(new MutableInt(infoEntry.getKey()),infoEntry.getValue());
				}
			}
			retMap.put(key, newValue);
		}
		return retMap;
	}
}
