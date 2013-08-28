package com.renren.hydra.util;

import java.util.Map;

import org.apache.lucene.search.Query;

import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.filter.Filter;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.client.SearchType;

public class HydraRequestBuilder {

	private HydraRequest _req;

	public HydraRequestBuilder() {
		clear();
	}

	public void applySort(Sort sort) {
		if (sort != null) {
			_req.setSort(sort);
		}
	}

	public void setCount(int count) {
		_req.setCount(count);
	}

	public void setUserId(int id) {
		_req.setUserId(id);
	}

	public void setFriendsInfoBytes(Map<Integer, byte[] > friendsInfo) {
                _req.setFriendsInfoBytes(friendsInfo);
        }
	
	public void setFriendsInfo(Map<Integer, Map<Integer,Short> > friendsInfo) {
        _req.setFriendsInfo(friendsInfo);
	}
	
	public void setUserInfo(Map<String, String> userInfo) {
		_req.setUserInfo(userInfo);
	}

	public void setSearchType(SearchType searchType) {
		_req.setSearchType(searchType);
	}

	public void setQuery(Query query) {
		if (query != null) {
			_req.setQuery(query);
		}
	}
	
	public void setQString(String queryString) {
		if (queryString != null) {
			_req.setQString(queryString);
		}
	}

	public void setShowExplanation(boolean isShow) {
		_req.setShowExplanation(isShow);
	}

	public void setFillAttribute(boolean fillAttribute) {
		_req.setFillAttribute(fillAttribute);
	}

	public void clear() {
		_req = new HydraRequest();
		_req.setCount(5);
	}

	public HydraRequest getRequest() {
		return _req;
	}

	public void setFilter(Filter filter) {
		if (filter != null)
			_req.setFilter(filter);
	}
}
