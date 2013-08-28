package com.renren.hydra.search;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.lucene.search.Query;

import com.renren.hydra.search.filter.Filter;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.search.sort.SortField;
import com.renren.hydra.client.SearchType;

public class HydraRequest implements Serializable {

	private static final long serialVersionUID = 1L;

	private long tid = -1;

	private int _count;
	private int _partitionSize;

	private boolean _showExplanation;

	private int _userId;
	private Set<Integer> _partitions;

	private Query _query;
	private String _qString;
	
	private Sort _sort;
	private boolean fillAttribute;

	private Filter filter;

	private Map<Integer, byte[] > _friendsInfoBytes;
	private Map<Integer, Map<Integer, Short>> _friendsInfo;
	private Map<Integer, Map<MutableInt, Short>> _friendsInfoFV;
	private Map<String, String> _userInfo;
	private SearchType _searchType;

	public HydraRequest() {
		_count = 0;
		_partitionSize = 0;
		_showExplanation = false;
		_userId = -1;
		_partitions = null;
		_query = null;
		_qString = null;
		_sort = null;
		this.fillAttribute = true;
		this.filter = null;
		this._userInfo = null;
		this._searchType = SearchType.All;
		this._friendsInfo = null;
		this._friendsInfoBytes = null;
	}

	public boolean isFillAttribute() {
		return fillAttribute;
	}

	public void setFillAttribute(boolean fillAttribute) {
		this.fillAttribute = fillAttribute;
	}

	public final long getTid() {
		return tid;
	}

	public final void setTid(long tid) {
		this.tid = tid;
	}

	public void setUserId(int userId) {
		_userId = userId;
	}

	public int getUserId() {
		return _userId;
	}

	public void setQuery(Query query) {
		_query = query;
	}

	public Query getQuery() {
		return _query;
	}

	public String getQString() {
		return _qString;
	}

	public void setQString(String qStr) {
		this._qString = qStr;
	}
	

	public int getCount() {
		return _count;
	}

	public void setCount(int count) {
		_count = count;
	}

	public boolean isShowExplanation() {
		return _showExplanation;
	}

	public void setShowExplanation(boolean showExplanation) {
		_showExplanation = showExplanation;
	}

	public void setPartitions(Set<Integer> partitions) {
		_partitions = partitions;
	}

	public Set<Integer> getPartitions() {
		return _partitions;
	}

	public void setPartitionSize(int partitionSize) {
		_partitionSize = partitionSize;
	}

	public int getPartitionSize() {
		return _partitionSize;
	}


	public void setSort(Sort sort){
		this._sort = sort;
	}
	
	public Sort getSort() {
		return _sort;
	}

	public void setSort(SortField[] sorts) {
		this._sort = new Sort(sorts);
	}
	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	public void setFriendsInfoBytes(Map<Integer, byte[] > friendsInfo) {
		this._friendsInfoBytes = friendsInfo;
	}
	
	public Map<Integer, byte[] >  getFriendsInfoBytes() {
		return this._friendsInfoBytes;
	}

	public void setFriendsInfo(Map<Integer, Map<Integer, Short>> friendInfo){
		this._friendsInfo = friendInfo;
	}
	
	public Map<Integer, Map<Integer, Short>> getFriendsInfo() {
		return _friendsInfo;
	}	
	
	public void setFriendsInfoFV(Map<Integer, Map<MutableInt, Short>> friendInfo){
		this._friendsInfoFV = friendInfo;
	}
	
	public Map<Integer, Map<MutableInt, Short>> getFriendsInfoFV() {
		return this._friendsInfoFV;
	}	

	public void setUserInfo(Map<String, String> userInfo) {
		this._userInfo = userInfo;
	}

	public Map<String, String> getUserInfo() {
		return _userInfo;
	}

	public void setSearchType(SearchType searchType) {
		_searchType = searchType;
	}

	public SearchType getSearchType() {
		return _searchType;
	}
	

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof HydraRequest))
			return false;
		HydraRequest b = (HydraRequest) o;

		if (getCount() != b.getCount())
			return false;
		if (!getSort().equals(b.getSort()))
			return false;
		if (getQuery() == null) {
			if (b.getQuery() != null)
				return false;
		} else {
			if (!getQuery().toString().equals(b.getQuery().toString()))
				return false;
		}
		if (getPartitions() == null) {
			if (b.getPartitions() != null)
				return false;
		} else {
			if (!setsAreEqual(getPartitions(), b.getPartitions()))
				return false;
		}
		
		if (_userId != b.getUserId()) {
			return false;
		}

		return true;
	}

	private <T> boolean setsAreEqual(Set<T> a, Set<T> b) {
		if (a.size() != b.size())
			return false;

		Iterator<T> iter = a.iterator();
		while (iter.hasNext()) {
			T val = iter.next();
			if (!b.contains(val))
				return false;
		}

		return true;
	}
}
