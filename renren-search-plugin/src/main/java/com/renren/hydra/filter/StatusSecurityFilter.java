package com.renren.hydra.filter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.Version;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieIndexReader.SubReaderAccessor;
import proj.zoie.api.ZoieIndexReader.SubReaderInfo;
import proj.zoie.impl.indexing.ZoieSystem;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.client.Condition;
import com.renren.hydra.client.SearchType;
import com.renren.hydra.config.HydraConfParams;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.parser.QueryConditionParser;
import com.renren.hydra.search.parser.QueryProcessUtil;
import com.renren.hydra.search.scorer.HydraScorer;
import com.renren.hydra.searcher.core.HydraCore;
import com.renren.hydra.searcher.core.search.HydraIndexSearcher;
import com.renren.hydra.searcher.core.search.filter.ConstantFilter;
import com.renren.hydra.thirdparty.zkmanager2.NodeDataListener;
import com.renren.hydra.thirdparty.zkmanager2.ZkCallback;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.hydra.util.AnalyzerPoolHolder;
import com.renren.hydra.util.BitMap;
import com.renren.hydra.util.SearchFlowFactory;
import com.xiaonei.xce.XceAdapter;

public class StatusSecurityFilter extends ConstantFilter {
	private static final Logger logger = Logger.getLogger(StatusSecurityFilter.class);

	private static final String DATA_NODE_PATH = "/search2/security_data";
	private static final String USER_NODE_PATH = "/search2/security_user";
	private static final String INVALID_USER_NODE_PATH = "/search2/invalid_user";
	private static final String DB_SOURCE_WORDS = "audit_content";
	private static final String DB_SOURCE_USERS = "search";
	private static final String DB_SOURCE_USER_PASSPORT = "user_passport";
	private Set<Long> uids;
	private BitMap userIds;
	private BitMap invalidIds;
	//private QueryConditionParser parser;
	private ZoieSystem zoie;
	private IndexReader[] _subReaders;
	private MultiReader _reader;
	private SubReaderAccessor<ZoieIndexReader<IndexReader>> _subReaderAccessor;
	private int partition;
	private int delay;
	private int period;
	private Schema _schema;
	private ZkManager zkManager;
	
	private int minSlop = 0;
	private int maxSlop = Integer.MAX_VALUE;
	private int minSlopTermLength = 2;
	private int minSlopWordLength = 3;
	private int scoreFieldCnt = 0;
	
	private AnalyzerPoolHolder analyzerPoolHolder;

	public StatusSecurityFilter(String filterName, HydraCore hydraCore, int partition) {
		super(filterName);
		uids = Collections.emptySet();
		userIds = new BitMap(0x7FFFFFFF);
		invalidIds = new BitMap(0x7FFFFFFF);

		_schema = hydraCore.getSchema();
		zoie = hydraCore.getZoieSystem(partition);
		HydraConfig config = hydraCore.getConfig();
		delay = config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_DELAY, 0);
		period = config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_PERIOD, 86400000);
		this.partition = partition;

		this.analyzerPoolHolder = AnalyzerPoolHolder.getInstance();
		
		minSlop = config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_MIN_SLOP, 0);
		
		this.scoreFieldCnt = _schema.getNumScoreField();
		if(this.scoreFieldCnt<0)
			this.scoreFieldCnt=1;
		
		maxSlop = config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_MAX_SLOP, Integer.MAX_VALUE);
		
		this.minSlopTermLength = config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_SLOP_TERMLENGTH, 2);
		
		this.minSlopWordLength = config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_SLOP_TERMLENGTH, 3);

		logger.info("loaddata: delay: " + delay + ", period: " + period);
		new LoadData().start();
		this.zkManager = ZkManager.getInstance(HydraConfig.getInstance().getZkProperties().getString("address"));
		registry();

	}

	public void registry() {
		logger.debug("registry security filter");
		zkManager.subscribeDataChanges(DATA_NODE_PATH,
				new NodeDataListener(new FilterUidsLoadListener()));
		zkManager.subscribeDataChanges(USER_NODE_PATH,
				new NodeDataListener(new FilterUserIdsLoadListener()));
		zkManager.subscribeDataChanges(INVALID_USER_NODE_PATH,
				new NodeDataListener(new FilterInvalidUserIdsLoadListener()));
	}

	private class FilterUidsLoadListener implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) {
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
			try {
				loadFilterWords();
			} catch (Exception e) {
				logger.error("load security data fail.", e);
			}
		}

		@Override
		public void handleDataDeleted(String dataPath) {
		}
	}

	private class FilterUserIdsLoadListener implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) {
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
			try {
				loadFilterUsers();
			} catch (Exception e) {
				logger.error("load security data fail.", e);
			}
		}

		@Override
		public void handleDataDeleted(String dataPath) {
		}
	}

	private class FilterInvalidUserIdsLoadListener implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath,
				List<String> currentChilds) {
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
			try {
				loadInvalidUsers();
			} catch (Exception e) {
				logger.error("load security data fail.", e);
			}
		}

		@Override
		public void handleDataDeleted(String dataPath) {
		}
	}

	public Set<Long> getUnsecureUids() {
		return uids;
	}

	public BitMap getUnsecureUsers() {
		return userIds;
	}

	public BitMap getInvalidUsers() {
		return invalidIds;
	}

	private void loadFilterUsers() throws SQLException {
		int count = 0;
		BitMap userSet = new BitMap(0x7FFFFFFF);
		logger.info("start to load private_status user from DB");
		Connection conn = XceAdapter.getInstance().getReadConnection(
				DB_SOURCE_USERS);
		if (conn == null) {
			logger.error("filter user database connection is null");
			return;
		}
		Statement stat = conn.createStatement();
		String sql = "select uid from private_status_uid limit 100000";
		ResultSet rs = stat.executeQuery(sql);
		while (rs.next()) {
			int id = rs.getInt("uid");
			if (id != 0) {
				userSet.set(id, true);
				count++;
			}
		}
		stat.close();
		conn.close();

		synchronized (userIds) {
			this.userIds = userSet;
		}

		logger.info("load " + count + " users from private_status_uid table");
	}

	/**
	 * @throws IOException
	 */
	private void loadInvalidUsers() throws IOException {
		logger.info("start to load invalid users from file.");

		BitMap forbiddenSet = new BitMap(0x7FFFFFFF);
		String path = "/data/xce/invalid_user";
		File file = new File(path);
		if (!file.exists()) {
			return;
		}
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line;

		do {
			line = bufferedReader.readLine();
			if (line == null || line.equals("")) {
				continue;
			} else {
				int id = Integer.parseInt(line);
				forbiddenSet.set(id, true);
			}
		} while (line != null);

		synchronized (invalidIds) {
			this.invalidIds = forbiddenSet;
		}
		logger.info("finished to load invalid users from file.");
	}
	
	private Query processQuery(String word){
		if(word==null)
			return null;
		word = QueryParser.escape(word.trim());
		if("".equals(word))
			return null;
		
		Analyzer analyzer = analyzerPoolHolder.getAnalyzerPool().getAnalyzer();
		Query query = null;
		try {
			QueryParser parser = new MultiFieldQueryParser(Version.LUCENE_30,_schema.getScoreFiledNames(),analyzer);
			parser.setDefaultOperator(Operator.AND);
			query = parser.parse(word);
		} catch (ParseException e) {
			logger.error("parser query error for word:"+word);
		}finally{
			this.analyzerPoolHolder.getAnalyzerPool().giveBackAnalyzer(analyzer);
		}
		if(query==null){
			logger.warn("query is null for word:"+word);
			return null;
		}
		Set<Term> termSets = new HashSet<Term>();
		query.extractTerms(termSets);
		int termCnt = termSets.size()/this.scoreFieldCnt;
		if(termCnt==1)
			return query;
		
		if(termCnt<=this.minSlopTermLength && word.length()<=this.minSlopWordLength && word.split("\\s+").length<2){
			logger.debug("termCnt is :"+termCnt+"\tphrase query process:"+word+"\t query is:"+query);
			QueryProcessUtil.processPhraseQuery(query, this.minSlop);
		}else
			QueryProcessUtil.processPhraseQuery(query, this.maxSlop);
		
		return query;
	}

	private void loadFilterWords() throws Exception {
		List<ZoieIndexReader<IndexReader>> readerList = zoie.getIndexReaders();
		_subReaderAccessor = ZoieIndexReader.getSubReaderAccessor(readerList);
		_reader = new MultiReader(readerList.toArray(new IndexReader[readerList
				.size()]), false);

		List<IndexReader> subReadersList = new ArrayList<IndexReader>();
		ReaderUtil.gatherSubReaders(subReadersList, _reader);
		_subReaders = subReadersList.toArray(new IndexReader[subReadersList
				.size()]);

		logger.info("start to load security data from DB");

		// 为了让uids有更好的效率，使用局部变量
		Set<Long> uidSet = new HashSet<Long>();

		// 直接从数据库中读出需要屏蔽的关键词
		Connection conn = XceAdapter.getInstance().getReadConnection(
				DB_SOURCE_WORDS);
		if (conn == null) {
			logger.error("filter word database connection is null");
			return;
		}
		Statement stat = conn.createStatement();
		String sql = "select id, keyword from filter_search_data where disable=0 and (filter_end_time is NULL or filter_end_time > now()) limit 100000";
		ResultSet rs = stat.executeQuery(sql);
		int count = 0;
		while (rs.next()) {
			count++;
			String word = rs.getString("keyword");
			logger.debug("word : " + word);
			
			Query query = processQuery(word);
			if(query==null)
				continue;
	
			HydraIndexSearcher searcher = new HydraIndexSearcher(_schema,
					_reader, 0, SearchType.All,null,null,null,null, null, null);
			Weight weight = query.weight(searcher);
			int baseDocId = 0;
			// int indexCount = 0;
			for (int i = 0; i < _subReaders.length; i++) {
				Scorer scorer = weight.scorer(
						_subReaders[i], false, false);
				if(scorer==null)
					continue;
				while (true) {
					int localDocId = scorer.nextDoc();
					if (localDocId == DocIdSetIterator.NO_MORE_DOCS) {
						break;
					}

					SubReaderInfo<ZoieIndexReader<IndexReader>> readerInfo = _subReaderAccessor
							.getSubReaderInfo(localDocId + baseDocId);
					ZoieIndexReader<IndexReader> zoieIndexReader = readerInfo.subreader;
					
					long uid = (long) zoieIndexReader
							.getUID(readerInfo.subdocid);
					// indexCount++;
					uidSet.add(uid);
				}

				baseDocId += _subReaders[i].maxDoc();
			}
		}
		stat.close();
		conn.close();
		// 更新uids
		if (!uidSet.isEmpty()) {
			synchronized (uids) {
				uids = uidSet;
			}
		}
		uidSet = null;

		if (logger.isInfoEnabled()) {
			logger.info("load " + count + " data from DB in partition "
					+ partition + ", filter " + uids.size() + " doc");
		}
		logger.info("finish load security data from DB");
	}

	/**
	 * load filter data 的线程
	 * 
	 * @author benjamin
	 * 
	 */
	private class LoadData extends Thread {
		@Override
		public void run() {
			Timer timer = new Timer();

			timer.schedule(new LoadDataTask(), delay, period);
		}
	}

	/**
	 * load filter data 的定时任务
	 * 
	 * @author benjamin
	 * 
	 */
	private class LoadDataTask extends TimerTask {
		@Override
		public void run() {
			try {
				loadFilterWords();
				loadFilterUsers();
			} catch (Exception e) {
				logger.error("load filter data error. ", e);
			}
		}
	}

	@Override
	public boolean filter(long docId, OnlineAttributeData onlineAttributes,
			OfflineAttributeData offlineAttributes, Map<MutableInt,Short> friendsInfo) {
		if (null != this.uids && this.uids.contains(docId)) {
			return true;
		}
		int userid = onlineAttributes.getUserId();
		if (userIds.get(userid)) {
			if (friendsInfo == null) {
				return true;
			}
			if (!friendsInfo.containsKey(userid)) {
				return true;
			}
		}
		return false;
	}
}

