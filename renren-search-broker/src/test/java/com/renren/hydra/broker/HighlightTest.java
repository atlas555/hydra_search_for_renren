package com.renren.hydra.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.renren.hydra.util.HighlightUtil;
import com.renren.search.analyzer.standard.AresStandardAnalyzer;

public class HighlightTest {
	private static Logger logger = Logger.getLogger(BrokerI.class);

	private Analyzer highlightAnalyzer;
	private HighlightUtil highlightUtil;

	@Before
	public void setUp() throws Exception {
		highlightAnalyzer = new AresStandardAnalyzer();
		this.highlightUtil = new HighlightUtil(this.highlightAnalyzer);
	}

	private static class TestClient extends Thread {
		private HighlightUtil highlightUtil;

		public TestClient(HighlightUtil util) {
			highlightUtil = util;
		}

		long uididx = 0l;

		public void putKv(Map<Long, JSONObject> jsonMap, String str) {
			try {
				JSONObject j = new JSONObject(str);
				jsonMap.put(uididx++, j);
			} catch (JSONException e) {
				logger.info(e);
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			String first = null;
			while (true) {
				Set<String> highlightSummaryFields = new HashSet<String>();

//				highlightSummaryFields.add("title");
				highlightSummaryFields.add("summary");
				Map<Long, JSONObject> jsonMap = new HashMap<Long, JSONObject>();

				putKv(jsonMap, "{title:\"中国北京的人\", summary:美国人}");
//				putKv(jsonMap, "{title:中国北京的人中,summary:\"眉飞色舞\"}");
//				putKv(jsonMap, "{title:我爱北京天安门,summary:this is ok}");
				
				this.highlightUtil.highlight("北京人",jsonMap, 
						highlightSummaryFields.toArray(new String[0]),true);
//				logger.info("json string:" + jsonMap.get(1L).toString());
				if(first == null){
					first = jsonMap.get(0L).toString();
				} else {
					if(first.compareTo(jsonMap.get(0L).toString() )!= 0){
						logger.info("shit");
					}
				}
				logger.info("output:" + jsonMap + " tid:"  +
														 Thread.currentThread
														 ().getId()
														 );
				uididx = 0L;
			}
			// TestClient t1 = new TestClient(highlightAnalyzer);
			// t1.start();
		}

	}

	@Test
	public void testHighlight() {
		int cnt = 10;
		for (int i = 0; i < 10; ++i) {
			TestClient t1 = new TestClient(highlightUtil);
		//	t1.start();
//			logger.info("start i:" + i);
		}
	}
}
