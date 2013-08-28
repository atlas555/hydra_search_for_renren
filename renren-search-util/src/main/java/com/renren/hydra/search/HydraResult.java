package com.renren.hydra.search;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.json.JSONObject;

import com.renren.hydra.search.scorer.HydraScoreDoc;

public class HydraResult implements Serializable 
{
	private static final long serialVersionUID = 1L;
  
	public static final HydraResult EMPTY_RESULT = new HydraResult();

	private String _parsedQuery = null;
  
	private Query query = null;
	
	private HydraScoreDoc[] _hits = null;

	private int _totalDocs = 0;
  
	private Map<Long, String > _jsonMap = null;
  
	private int _numHits = 0;

	private long _time = 0;

	private long _tid = -1;

	public boolean isEmpty() {
		return (_numHits == 0);
	}
	
	public void setContentMap(Map<Long,String> contentMap){
		this._jsonMap = contentMap;
	}
	

	public void setJsonMap(Map<Long,JSONObject> jsons){
		if(_jsonMap == null){
		  	_jsonMap = new HashMap<Long, String >();
		}

	  	for(Map.Entry<Long, JSONObject> e: jsons.entrySet()){
		  	_jsonMap.put(e.getKey(), e.getValue().toString());
		}
	}
  
	public Map<Long, JSONObject > getContent() throws Exception {
		if(_jsonMap == null) {
			return null;
		}
		Map<Long, JSONObject > res = new HashMap<Long, JSONObject > ();
		for(Map.Entry<Long, String> e: _jsonMap.entrySet()){
			res.put(e.getKey(), new JSONObject(e.getValue()));
		}
		return res;
	}

	public void setTid(long tid) {
		_tid = tid;
	}

	public long getTid() {
		return _tid;
	}

	public void setTime(long time) {
		_time = time;
	}

	public long getTime() {
		return _time;
	}

	public void setTotalDocs(int totalDocs) {
		_totalDocs = totalDocs;
	}

	public int getTotalDocs() {
		return _totalDocs;
	}

	public void setNumHits(int numHits) {
		_numHits = numHits;
	}

	public int getNumHits() {
		return _numHits;
	}

	public void setHits(HydraScoreDoc[] hits) {
		_hits = hits;
	}

	public HydraScoreDoc[] getHits()
	{
		return _hits;
	}	
  
	public void setQuery(Query query) {
		this.query = query;
	}
  
	public Query getQuery() {
		return this.query;
	}

	public void setParsedQuery(String query)
	{
		_parsedQuery = query;
	}

	public String getParsedQuery()
	{
		return _parsedQuery;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof HydraResult)) return false;
		HydraResult b = (HydraResult)o;

		if (!hydraHitsAreEqual(getHits(), b.getHits())) return false;
		if (!getParsedQuery().equals(b.getParsedQuery())) return false;

		if (getTime() != b.getTime()) return false;
		if (getNumHits() != getNumHits()) return false;
		if (getTotalDocs() != getTotalDocs()) return false;

		return true;
	}

	private boolean hydraHitsAreEqual(HydraScoreDoc[] a, HydraScoreDoc[] b) {
		if (a == null) return b == null;
		if (a.length != b.length) return false;

		for (int i = 0; i < a.length; i++) {
			if (a[i]._uid != b[i]._uid) return false;
			if (a[i].doc != b[i].doc) return false;
			if (a[i].score != b[i].score) return false;
		}

		return true;
	}

	private boolean rawFieldValuesAreEqual(Map<String,Object[]> a, Map<String,Object[]> b) {
		if (a == null) return b == null;
		if (a.size() != b.size()) return false;

		for (String key : a.keySet()) {
			if (!b.containsKey(key)) return false;
			if (!Arrays.equals(a.get(key), b.get(key))) return false;
		}

		return true;
	}

	private boolean fieldValuesAreEqual(Map<String,String[]> a, Map<String,String[]> b) {
		if (a == null) return b == null;
		if (a.size() != b.size()) return false;

		for (String key : a.keySet()) {
			if (!b.containsKey(key)) return false;
			if (!Arrays.equals(a.get(key), b.get(key))) return false;
		}

		return true;
	}

	private boolean storedFieldsAreEqual(Document a, Document b) {
		if (a == null) return b == null;
		return a.toString().equals(b.toString());
	}

}
