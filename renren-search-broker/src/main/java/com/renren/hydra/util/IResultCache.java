package com.renren.hydra.util;

import com.renren.hydra.search.HydraResult;

public interface IResultCache {
	public HydraResult get(String key);
	public void set(String key,HydraResult result);
	boolean init();
}
