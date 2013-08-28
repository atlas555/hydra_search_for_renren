package com.renren.hydra.util.impl;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import com.renren.hydra.search.HydraResult;
import com.renren.hydra.util.IResultCache;

public class MemcachedResultCache implements IResultCache {
	private static Logger logger = Logger.getLogger(MemcachedResultCache.class);
	
	private MemcachedClient client;
	private String serverList;
	private int expireTime;
	private int getTimeout;
	
	public MemcachedResultCache(String serverList, int expireTime,int getTimeout){
		this.serverList = serverList;
		this.expireTime = expireTime;
		this.getTimeout = getTimeout;
	}
	
	@Override
	public boolean init(){
		logger.info("init memcached client with serverlist:"+serverList+" expireTime:"+expireTime+" getTimeout:"+getTimeout);
		try {
			client = new MemcachedClient(AddrUtil.getAddresses(this.serverList));
		} catch (IOException e) {
			logger.error("init memcached error for server list:"+this.serverList,e);
			client.shutdown();
			return false;
		}
		return true;
	}

	@Override
	public HydraResult get(String key) {
		HydraResult result = null;
		if(key!=null && !key.isEmpty()){
			Future<Object> f=client.asyncGet(key);
			
			try {
				result=(HydraResult)f.get(getTimeout, TimeUnit.MILLISECONDS);
			} catch(Exception e) {
				f.cancel(true);
				logger.error("get "+key+" from cache failed",e);
			}
			if(logger.isDebugEnabled()){
				if(result!=null){
					logger.debug("get key from cache hit:"+key);
				}else{
					logger.debug("get key from cache miss:"+key);
				}
			}
		}
		return result;
	}

	@Override
	public void set(String key, HydraResult result) {
		if(key!=null&&result!=null){
			logger.debug("set key "+key);
			client.set(key, this.expireTime, result);
		}
	}
}
