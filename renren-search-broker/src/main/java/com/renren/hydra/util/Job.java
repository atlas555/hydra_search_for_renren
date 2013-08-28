package com.renren.hydra.util;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.renren.hydra.SearcherPrx;

public class Job {
	private static Logger logger = Logger.getLogger(Job.class);
	/** Searcher的一个代理 */
	protected SearcherPrx _searcher;

	protected int _offset;
	protected int _limit;
	protected byte[] _data;
	/** 存储搜索的结果 */
	protected Future<byte[]> _future;

	/**
	 * 构造函数
	 * 
	 * @param searcher
	 *            Searcher的一个代理
	 * @param offset
	 *            搜索的开始位置
	 * @param limit
	 *            搜索的结果数
	 */
	public Job(SearcherPrx searcher, int offset, int limit, byte[] data) {
		this._searcher = searcher;
		this._offset = offset;
		this._limit = limit;
		this._data = data;
	}

	public SearcherPrx getSearcher() {
		return _searcher;
	}

	public byte[] getData() {
		return _data;
	}

	public int getOffset() {
		return _offset;
	}

	public int getLimit() {
		return _limit;
	}

	public Future<byte[]> getFuture() {
		return _future;
	}

	public void setFuture(Future<byte[]> future) {
		this._future = future;
	}

	public byte[] getResult(long timeout) {
		try {
			return _future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.error("get search result error:"+this._searcher.ice_getConnection(),e);
		}
		return null;
	}
	
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(this._searcher.ice_getConnection());
		return sb.toString();
	}
}
