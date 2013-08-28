package com.renren.hydra.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.json.JSONObject;

import org.junit.Test;

public class BrokerITest {
 @Test
	public void getContentTest() throws Exception {
	 
	 List<String> uidList = new ArrayList<String>();
	 List<String> summaryList = new ArrayList<String>();
	 uidList.add("1");
	 uidList.add("2");
	 
	 String su1 = "{Title:'太 好 啊', Summary:'好 啊 的' , Text:'不 知 道 为 什 么 啊'}";
	 String su2 = "{Title:'人 人 啊 的',Summary:'北 啊 京' ,Text:'很 久 很 久 以 后 的 啊'}";
	 summaryList.add(su1);
	 summaryList.add(su2);
	 
//	Map<Long, JSONObject> jsons = BrokerI.multiThreadHighlight(uidList, summaryList, null);
	 
	 
//	System.out.println(jsons);
//	Assert.assertTrue("jsons", jsons);
//	Assert.assertNotNull(jsons);
	}
 
 
	public static class HighLightTaskTest implements Callable<Map<Long, JSONObject> > {
		//highlight线程
		private Long _uid = null;
		private String _summary = null;
		private Analyzer _analyzer;
		
//		Set<String> highlightSummaryFields = _schema.getHighlightSummaryFields();
//		Set<String> summaryFields = _schema.getNoHighlightSummaryFields();
		
		Set<String> highlightSummaryFields = new HashSet<String>();
		Set<String> summaryFields = new HashSet<String>();
		
		private Highlighter highlighter;
		
		Map<Long, JSONObject> jsons = new HashMap<Long, JSONObject>();
		
		public HighLightTaskTest(String uid, String summary, Query query) {
			this._uid = Long.parseLong(uid);
			this._summary = summary;
				
			String prefixHTML = "<font color='red'>";
			String suffixHTML = "</font>";
			SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(prefixHTML, suffixHTML);		
			
			String queryExpression = "啊";
			
			TermQuery t1 = new TermQuery(new Term("Title", queryExpression));
			TermQuery t2 = new TermQuery(new Term("Summary", queryExpression));
			TermQuery t3 = new TermQuery(new Term("Text", queryExpression));
			BooleanQuery _query = new BooleanQuery();
			_query.add(t1, BooleanClause.Occur.SHOULD);
			_query.add(t2, BooleanClause.Occur.SHOULD);
			_query.add(t3, BooleanClause.Occur.SHOULD);
//			processPhraseQuery(_query);
			
			highlighter = new Highlighter(simpleHTMLFormatter,
					new QueryScorer(_query));			
			
			
			_analyzer = new WhitespaceAnalyzer();//自己构造的analyzer
			
			highlightSummaryFields.add("Title");
			highlightSummaryFields.add("Summary");
			summaryFields.add("Text");
		}
		
		@Override
		public Map<Long, JSONObject> call() throws Exception {
			System.out.println("begin call");
			JSONObject dstJson = new JSONObject();
			System.out.println("s:"+_summary);
			System.out.println("in call,summary:"+_summary);
			JSONObject srcJson = new JSONObject(_summary);
			System.out.println("after srcJson");

//			logger.info("start to hightlight for uid " + _uid);
			
			for (Iterator it = highlightSummaryFields.iterator(); it.hasNext();) {
				String fieldname = (String) it.next();
				String fieldValue = srcJson.getString(fieldname);
				if (fieldValue == null) {
//					logger.warn("for uid " + _uid + ", no field " + fieldname+ " found.");
				}
//				logger.info("hightlight field: " + fieldname + ", value: "+ fieldValue);
				String t = highlighter.getBestFragment(_analyzer, fieldname,
						fieldValue);
//				logger.info("hightlight result: " + t);
				if (t == null) {
					dstJson.put(fieldname, fieldValue);
				} else {
					dstJson.put(fieldname, t);
				}
			}
			
//			logger.info("start to process all no highlight summary field for uid " + _uid);
			for (Iterator it = summaryFields.iterator(); it.hasNext();) {
				String fieldname = (String) it.next();
				String fieldValue = srcJson.getString(fieldname);
//				logger.info("add field: " + fieldname + ", with value: "+ fieldValue);
				dstJson.put(fieldname, fieldValue);
			}			
			jsons.put(_uid, dstJson);		
			return jsons;
		}
	}	

}