package com.renren.hydra.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.lucene.search.highlight.QueryTermScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.util.Version;
import org.json.JSONObject;

import com.renren.hydra.search.parser.QueryProcessUtil;

public class HighlightUtil {
	private static Logger logger = Logger.getLogger(HighlightUtil.class);

	private static final String prefixHTML = "<font color='red'>";
	private static final String suffixHTML = "</font>";
	private static final SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(
			prefixHTML, suffixHTML);
	public static final int HIGHLIGHT_TIME_OUT=3000;
	public static final int HIGHLIGHT_FRAGMENT_SIZE=200;
	
	private ExecutorService highlightExecutor; 
	private Analyzer analyzer;
	private int timeout;
	private int fragmentSize;
	private AnalyzerPoolHolder analyzerPoolHolder;

	public HighlightUtil(Analyzer analyzer){
		this(analyzer,HIGHLIGHT_TIME_OUT,HIGHLIGHT_FRAGMENT_SIZE);
	}
	
	public HighlightUtil(Analyzer analyzer,int timeout, int fragmentSize){
		this.analyzer = analyzer;
		int numProcessor = Runtime.getRuntime().availableProcessors();
		this.highlightExecutor = new ThreadPoolExecutor(numProcessor, numProcessor, 30L, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(2000),
				new ThreadPoolExecutor.DiscardOldestPolicy());
		this.timeout = timeout;
		this.fragmentSize = fragmentSize;
		this.analyzerPoolHolder = AnalyzerPoolHolder.getInstance();
	}

	
	private static class HighLightTask implements Callable<JSONObject> {
		private Analyzer analyzer;
		private JSONObject obj;
		private String[] fieldNames;
		private Highlighter highlighter;
		private boolean doSummary;
		private String queryStr;

		public HighLightTask(Query query, String queryStr, Analyzer analyzer,
				JSONObject obj, String[] fieldNames, boolean doSummary,
				int fragmentSize) {
			this.analyzer = analyzer;
			this.obj = obj;
			this.fieldNames = fieldNames;
			this.doSummary = doSummary;
			this.queryStr = queryStr;

			highlighter = new Highlighter(simpleHTMLFormatter,
					new QueryTermScorer(query));
			highlighter.setTextFragmenter(new SimpleFragmenter(fragmentSize));
			if (!this.doSummary) {
				highlighter.setTextFragmenter(new NullFragmenter());
			}
		}

		@Override
		public JSONObject call() throws Exception {
			for (String fieldName : fieldNames) {
				String fieldValue = this.obj.optString(fieldName);
				if (!fieldValue.equals("")) {
					Analyzer _analyzer = AnalyzerPoolHolder.getInstance().getAnalyzerPool().getAnalyzer();
					try{
						String ret = highlighter.getBestFragment(_analyzer,
								fieldName, fieldValue);
						if (ret == null) {
							logger.debug("hightlight fail : " + fieldValue);
						} else {
							obj.put(fieldName,ret);
						}
					}catch(Exception e){
						logger.error("highligh error:",e);
					}finally{
						AnalyzerPoolHolder.getInstance().getAnalyzerPool().giveBackAnalyzer(_analyzer);
					}
				}
			}
			return obj;
		}
	}
	
	public Query getQuery(String queryStr){
		Query query = null;
		if(queryStr == null || queryStr.equals(""))
			return query;
		
		Analyzer _analyzer = AnalyzerPoolHolder.getInstance().getAnalyzerPool().getAnalyzer();
		try {
			QueryParser queryParser = new QueryParser(Version.LUCENE_30, "text", _analyzer);
			query = queryParser.parse(queryStr);
		} catch (ParseException e1) {
			logger.error("parser query "+queryStr+" for hightlight failed",e1);
		}finally{
			AnalyzerPoolHolder.getInstance().getAnalyzerPool().giveBackAnalyzer(_analyzer);
		}
		if(query!=null)
			QueryProcessUtil.processPhraseQuery(query,Integer.MAX_VALUE);
		return query;
	}
	

	public void highlight(String queryStr,List<JSONObject> objList, String[] fields, boolean doSummary) {
		if (fields == null || fields.length == 0)
			return;
		Query query = getQuery(queryStr);
		if(query==null)
			return ;

		List<Future<JSONObject>> jsonsList = new ArrayList<Future<JSONObject>>();
		
		for (JSONObject obj : objList){
			Future<JSONObject> json = highlightExecutor.submit(new HighLightTask(query,queryStr,analyzer,
					obj, fields, doSummary, this.fragmentSize));
			jsonsList.add(json);
		}

		for (Future<JSONObject> fs : jsonsList) {
			try {
				fs.get(this.timeout,TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}
	
	public void highlight(String queryStr,Map<Long,JSONObject> objMap, String[] fields, boolean doSummary) {
		if (fields == null || fields.length == 0)
			return;
		
		Query query = getQuery(queryStr);
		
		if(query==null)
			return ;

		List<Future<JSONObject>> jsonsList = new ArrayList<Future<JSONObject>>();
		for (Map.Entry<Long,JSONObject> entry : objMap.entrySet()){
			Future<JSONObject> json = highlightExecutor.submit(new HighLightTask(query,queryStr,
					analyzer, entry.getValue(), fields, doSummary, this.fragmentSize));
			jsonsList.add(json);
		}

		for (Future<JSONObject> fs : jsonsList) {
			try {
				fs.get(this.timeout,TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

	}
	private static class HighlinghFilter {
		private static int preLength = prefixHTML.length();
		private static int suffLength = suffixHTML.length();

		private static class Info {
			private String content;
			private int startOffset;
			private int endOffset;

			public String getContent() {
				return content;
			}

			public void setContent(String content) {
				this.content = content;
			}

			public int getStartOffset() {
				return startOffset;
			}

			public void setStartOffset(int startOffset) {
				this.startOffset = startOffset;
			}

			public int getEndOffset() {
				return endOffset;
			}

			public void setEndOffset(int endOffset) {
				this.endOffset = endOffset;
			}

			public String toString() {
				return "[content=" + content + ",startOffset=" + startOffset
						+ ",endOffset=" + endOffset + "]";
			}

			public boolean equsals(Object other) {
				if (this == other)
					return true;
				if (other instanceof Info) {
					return (this.getContent().equals(((Info) other)
							.getContent()))
							&& (this.getStartOffset() == ((Info) other)
									.getStartOffset())
							&& (this.getEndOffset() == ((Info) other)
									.getEndOffset());
				} else {
					return false;
				}
			}
		}

		public HighlinghFilter() {
		}

		public static String filter(String content, String query) {
			// 将标红信息提取出来
			List<Info> contents = getContent(content);
			// 确定标红的单位
			List<Info> wordList = findMaxUnit(contents, query);
			// 剔除不需要标红部分
			String newContent = process(content, contents, wordList);
			return newContent;
		}

		private static List<Info> getContent(String content) {
			List<Info> contents = new ArrayList<Info>();
			if (content == null || content.trim().isEmpty())
				return contents;
			int preIndex = 0;
			int surIndex = 0;
			while ((preIndex = content.indexOf(prefixHTML, preIndex)) > -1) {
				surIndex = content.indexOf(suffixHTML, preIndex + preLength);
				if (surIndex > -1) {
					Info info = new Info();
					info.setContent(content.substring(preIndex + preLength,
							surIndex));
					info.setStartOffset(preIndex);
					info.setEndOffset(surIndex + suffLength);
					preIndex = surIndex + suffLength;
					contents.add(info);
				}
			}

			return contents;

		}

		private static List<Info> findMaxUnit(List<Info> contents, String query) {
			List<Info> maxUnits = new ArrayList<Info>();

			Map<Integer, Map<String, List<List<Info>>>> map = new HashMap<Integer, Map<String, List<List<Info>>>>();
			int index = 0;
			while (index < contents.size()) {
				List<Info> infos = findNearWords(contents, query, index);
				String word = composeWord(infos);
				int size = word.length();
				Map<String, List<List<Info>>> words = map.get(size);
				if (words == null) {
					words = new HashMap<String, List<List<Info>>>();
					map.put(size, words);
				}

				List<List<Info>> infoLists = words.get(word);
				if (infoLists == null) {
					infoLists = new ArrayList<List<Info>>();
				}
				infoLists.add(infos);
				words.put(word, infoLists);
				index += infos.size();
			}
			Map<Integer, List<String>> wordMap = new HashMap<Integer, List<String>>();
			for (int intKey : map.keySet()) {
				Map<String, List<List<Info>>> tmpMap = map.get(intKey);
				List<String> words = wordMap.get(intKey);
				if (words == null) {
					words = new ArrayList<String>();
					wordMap.put(intKey, words);
				}
				for (String strKey : tmpMap.keySet()) {
					words.add(strKey);
				}
			}
			List<String> wordList = findSuitableWrods(wordMap, query);
			if (wordList != null && !wordList.isEmpty()) {
				for (String word : wordList) {
					List<List<Info>> infosList = map.get(word.length()).get(
							word);
					for (List<Info> infos : infosList)
						maxUnits.addAll(infos);
				}
			}
			return maxUnits;
		}

		private static List<Info> findNearWords(List<Info> contents,
				String query, int index) {
			List<Info> nearWordsList = new ArrayList<Info>();
			while (index < contents.size()) {
				Info word = contents.get(index);
				nearWordsList.add(word);
				if (++index < contents.size()) {
					Info next = contents.get(index);
					if (word.getEndOffset() < next.getStartOffset()
							|| query.indexOf(word.getContent()
									+ next.getContent()) == -1)
						break;
				}
			}
			return nearWordsList;
		}

		private static String composeWord(List<Info> infos) {
			String word = "";
			for (Info info : infos) {
				word += info.getContent();
			}
			return word;
		}

		private static List<String> findSuitableWrods(
				Map<Integer, List<String>> wordMap, String query) {
			List<String> words = new ArrayList<String>();
			Integer[] keys = wordMap.keySet().toArray(
					new Integer[wordMap.size()]);
			Arrays.sort(keys);
			int key = -1;
			int i = 0;
			int length = query.length();
			for (; i < keys.length; i++) {
				if (keys[i] <= length) {
					if (i + 1 >= keys.length || keys[i + 1] > length) {
						key = keys[i];
					}
				}
			}
			if (key == -1) {
				return null;
			}
			List<String> wordList = wordMap.get(key);
			if (wordList == null || wordList.isEmpty()) {
				return null;
			} else {
				String word = wordList.remove(0);
				int index = query.indexOf(word);
				if (index > -1) {
					String preQuery = query.substring(0, index);
					String surQuery = query.substring(index + word.length());
					words.add(word);

					if (preQuery != null && !preQuery.isEmpty()) {
						List<String> preWords = findSuitableWrods(wordMap,
								preQuery);
						if (preWords != null && !preWords.isEmpty()) {
							words.addAll(preWords);
						}
					}

					if (surQuery != null && !surQuery.isEmpty()) {
						List<String> surWords = findSuitableWrods(wordMap,
								surQuery);
						if (surWords != null && !surWords.isEmpty()) {
							words.addAll(surWords);
						}
					}
				} else {
					words = findSuitableWrods(wordMap, query);
				}
				return words;
			}
		}

		private static String process(String content, List<Info> allInofs,
				List<Info> retainedInfos) {
			StringBuilder sb = new StringBuilder(content);

			int filted = 0;
			for (Info all : allInofs) {
				boolean included = false;
				for (Info retained : retainedInfos) {
					if (all.equsals(retained)) {
						included = true;
						break;
					}
				}
				if (!included) {
					int start = all.startOffset - filted;
					sb.delete(start, start + preLength);
					filted += preLength;
					int end = all.endOffset - filted;
					sb.delete(end - suffLength, end);
					filted += suffLength;
				}
			}

			return sb.toString();
		}
	}
}
