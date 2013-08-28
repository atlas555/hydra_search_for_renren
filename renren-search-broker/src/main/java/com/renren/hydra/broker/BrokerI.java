package com.renren.hydra.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConversionException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;

import Ice.Current;

import org.json.JSONException;
import org.json.JSONObject;

import com.renren.cluster.ClusterException.ClusterConnException;
import com.renren.cluster.client.redis.RedisClusterPoolClient;
import com.renren.hydra.SearcherPrx;
import com.renren.hydra._BrokerDisp;
import com.renren.hydra.client.Condition;
import com.renren.hydra.client.SearchType;
import com.renren.hydra.config.HydraConfParams;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.HydraResult;
import com.renren.hydra.search.ResultMerger;
import com.renren.hydra.search.parser.QueryConditionParser;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.thirdparty.zkmanager2.Broker;
import com.renren.hydra.thirdparty.zkmanager2.SessionExpireListener;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.hydra.util.AnalyzerPoolHolder;
import com.renren.hydra.util.Compress;
import com.renren.hydra.util.HighlightUtil;
import com.renren.hydra.util.HydraRequestBuilder;
import com.renren.hydra.util.IResultCache;
import com.renren.hydra.util.Job;
import com.renren.hydra.util.ResultCacheUtil;
import com.renren.hydra.util.SearchFlowFactory;
import com.renren.hydra.util.SearchThreadPool;
import com.renren.hydra.util.SearcherPool2;
import com.renren.hydra.util.SerializableTool;
import com.renren.hydra.util.impl.MemcachedResultCache;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil.LoadWeight;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil.FixedLoadWeight;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil.SendMetricsJob;
import com.renren.hydra.util.loadbalance.Policy;
import com.renren.hydra.util.loadbalance.PolicyFactory;
import com.renren.hydra.util.TimeCost;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.TimerMetric;

public class BrokerI extends _BrokerDisp {
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(BrokerI.class);
	private static final TimerMetric SEARCH_TIMER = Metrics.newTimer(
			BrokerI.class, "SEARCH_TIMER", TimeUnit.MILLISECONDS,
			TimeUnit.SECONDS);
	private final static HistogramMetric AVG_TOTAL_DOC = Metrics.newHistogram(
			BrokerI.class, "AVG_TOTAL_DOC");
	private final static HistogramMetric AVG_RETURN_DOC = Metrics.newHistogram(
			BrokerI.class, "AVG_RETURN_DOC");
	private static ScheduledExecutorService executor;
	private static final long JOB_INTERVAL = 5;
	private static final int TIME_OUT = 10000;

	private String _ip;
	private String _port;
	private String _business;
	private Policy policy;
	private SearchThreadPool _pool;
	//private QueryConditionParser _parser;
	private HydraConfig _config;
	private Schema _schema;
	private ResultMerger _resultMerger;
	//private Analyzer _analyzer;
	private RedisClusterPoolClient _redisClient;
	private ZkManager zkManager;
	private SearcherPool2 searcherPool;
	private String[] _highlightSummaryFields;
	private boolean _needFragment = true;
	private int _fragmentSize = 200;
	private HighlightUtil highlightUtil;
	private boolean hasHighlightFields = false;
	private boolean highlightEnable = true;
	//private HydraAnalyzerPool analyzerPool;
	private AnalyzerPoolHolder analyzerPoolHolder;
	private IResultCache resultCache;
	private boolean resultCacheEnable = false; 

	public BrokerI() {

	}

	public BrokerI(HydraConfig config, String ip, String port, String business) {
		this._ip = ip;
		this._port = port;
		this._business = business;
		this._pool = new SearchThreadPool();
		this._resultMerger = new ResultMerger();
		this._config = config;
		if (config != null) {
			this._schema = config.getSchema();
			logger.info(this._schema);
		}
	}

	public boolean init() {
		logger.info("begin to init parser.");

		if (_schema == null) {
			logger.error("cannot find schema for business: " + _business);
			return false;
		}

		policy = PolicyFactory.createPolicy(_config
				.getHydraConfig()
				.subset(LoadBalanceUtil.CONFIG_PREFIX)
				.getString(LoadBalanceUtil.CONFIG_POSTFIX,
						LoadBalanceUtil.DEFAULT_POLICY));

		searcherPool = new SearcherPool2();
		searcherPool.init(_business, policy);

		/*_analyzer = SearchFlowFactory.createSearchAnalyzer(_schema);
		if (_analyzer == null) {
			logger.error("create analyzer failed");
			return false;
		}

	
		
		_parser = (QueryConditionParser) SearchFlowFactory.createQueryParser(
				_schema, null);
		if (_parser == null) {
			logger.error("create query parser failed");
			return false;
		}*/

		if (!initHighlight()) {
			return false;
		}

		Configuration businessConfig = _config.getHydraConfig();
		
		/*int analyzerPoolSize = 100;
		try{
			analyzerPoolSize = businessConfig.getInt("hydra.analyzer.pool.size",100);
		}catch(ConversionException e){
			logger.error(e);
			analyzerPoolSize = 100;
		}
		
		String analyzerPoolType = businessConfig.getString("hydra.analyzer.pool.type","tc");
		logger.info("analyzer pool type is "+analyzerPoolType+" analyzer pool size is "+analyzerPoolSize);
		if(analyzerPoolType.equals("ares"))
			this.analyzerPool = new HydraAresAnalyzerPool(analyzerPoolSize);
		else
			this.analyzerPool = new HydraTCAnalyzerPool(analyzerPoolSize);*/
		if(!AnalyzerPoolHolder.initAnalyzerPool())
			return false;
		
		analyzerPoolHolder = AnalyzerPoolHolder.getInstance();
		
		Configuration zkProperties = _config.getZkProperties();
		if (zkProperties == null) {
			logger.warn("zkProperties null!");
			return false;
		}

		zkManager = ZkManager.getInstance(zkProperties.getString("address"));
		if (zkManager == null)
			return false;

		// 初始化redis
		String redisZKServer = businessConfig
				.getString(HydraConfParams.HYDRA_REDIS_ZOOKEEPER_SERVER);

		if (redisZKServer != null && !redisZKServer.isEmpty()) {
			String redisName = businessConfig.getString(
					HydraConfParams.HYDRA_REDIS_NAME, "search");
			if (redisName == null) {
				logger.error("cannot find config: "
						+ HydraConfParams.HYDRA_REDIS_NAME);
				return false;
			}

			logger.info("redis server name: " + redisName);
			logger.info("redis zookeeper server: " + redisZKServer);

			try {
				_redisClient = new RedisClusterPoolClient(redisName,
						redisZKServer);
				_redisClient.init();
			} catch (ClusterConnException e) {
				logger.error("create redis client fail. " + e.getMessage(), e);
			}
			if (null == _redisClient) {
				logger.error("_redisClient is null");
				return false;
			}
		} else {
			_redisClient = null;
			logger.info("disable redis");
		}
		
		if (!initResultCache()) {
			logger.info("init result cache failed");
			this.resultCache = null;
			return false;
		}
		logger.info("init result cache ok");
		
		return true;
	}

	public boolean initHighlight() {
		this._highlightSummaryFields = _schema.getHighlightSummaryFields()
				.toArray(new String[0]);
	
		if (this._highlightSummaryFields == null
				|| this._highlightSummaryFields.length == 0) {
			this.hasHighlightFields = false;
			return true;
		}
		this.hasHighlightFields = true;
		int timeout = HighlightUtil.HIGHLIGHT_TIME_OUT;
		try {
			this.highlightEnable = _config.getHydraConfig().getBoolean(
					HydraConfParams.HYDRA_HIGHLIGHT_ENABLE, true);
		} catch (ConversionException e) {
			logger.error(e);
		}
		if (!this.highlightEnable)
			return true;

		try {
			_needFragment = _config.getHydraConfig().getBoolean(
					HydraConfParams.HYDRA_HIGHLIGHT_SEGMENT_ENABLE, false);
			_fragmentSize = _config.getHydraConfig().getInt(
					HydraConfParams.HYDRA_HIGHLIGHT_SEGMENT_SIZE,
					HighlightUtil.HIGHLIGHT_FRAGMENT_SIZE);
			timeout = _config.getHydraConfig().getInt(
					HydraConfParams.HYDRA_HIGHLIGHT_TIMEOUT,
					HighlightUtil.HIGHLIGHT_TIME_OUT);
		} catch (ConversionException e) {
			logger.error(e);
		}

		Analyzer highlightAnalyzer = null;
		/*Analyzer highlightAnalyzer = SearchFlowFactory
				.createHighlightAnalyzer(_schema);
		if (highlightAnalyzer == null) {
			logger.error("create highlight analyzer failed");
			return false;
		}*/

		this.highlightUtil = new HighlightUtil(highlightAnalyzer, timeout,
				this._fragmentSize);
		if (this.highlightUtil == null) {
			logger.error("create highlight util failed");
			return false;
		}
		return true;
	}

	public boolean initResultCache(){
		try {
			this.resultCacheEnable = _config.getHydraConfig().getBoolean(
					HydraConfParams.HYDRA_RESULT_CACHE_ENABLE, false);
		} catch (ConversionException e) {
			logger.error(e);
		}
		if (!this.resultCacheEnable)
			return true;

		int expire = 10;
		String serverList = null;
		int getTimeout = 500;
		try {
			expire = _config.getHydraConfig().getInt(
					HydraConfParams.HYDRA_RESULT_CACHE_EXPIRE,10);
			serverList = _config.getHydraConfig().getString(
					HydraConfParams.HYDRA_RESULT_CACHE_SERVER_LIST, "");
			getTimeout = _config.getHydraConfig().getInt(
					HydraConfParams.HYDRA_RESULT_CACHE_GET_TIMEOUT,500);
		} catch (ConversionException e) {
			logger.error(e);
		}
		
		if(null == serverList || serverList.isEmpty())
			return false;
		this.resultCache = new MemcachedResultCache(serverList,expire,getTimeout);
		return this.resultCache.init();
	}
	
	public void registry() {
		Broker broker = new Broker(_ip, _port, _business);
		try {
			zkManager.deleteNode(broker.getAbsolutePath());
			Thread.sleep(2000);
			zkManager.createNode(Broker.getParentPath(), broker);
			zkManager.subscribeStateChanges(new SessionExpireListener(Broker
					.getParentPath(), broker, zkManager));
			if (LoadBalanceUtil.isFeedback(_config.getHydraConfig().getString(
					HydraConfParams.HYDRA_SEARCHER_LOADBALANCE, null))) {
				long interval = _config.getHydraConfig().getLong(
						HydraConfParams.HYDRA_LOADBALANCE_HEARTBEAT_INTVERVAL,
						JOB_INTERVAL);
				LoadWeight weight = new FixedLoadWeight(_config
						.getHydraConfig().getDouble(
								HydraConfParams.HYDRA_LOADBALANCE_WEIGHT, 1.0));
				executor = Executors.newSingleThreadScheduledExecutor();
				executor.scheduleAtFixedRate(new SendMetricsJob(zkManager,
						broker.getAbsolutePath(), SEARCH_TIMER, weight),
						interval, interval, TimeUnit.SECONDS);
				logger.info("Info of  SendMetricsJob:{LoadWeight="
						+ weight.getClass() + ",interval=" + interval
						+ ",TimeUnit=SECONDS}");
			}
			logger.info("register " + broker.toString());
		} catch (Exception e) {
			logger.error("registry fail. " + e.getMessage(), e);
		}
	}

	public void unregistry() {
		Broker broker = new Broker(_ip, _port, _business);
		try {
			zkManager.deleteNode(broker.getAbsolutePath());
			logger.info("unregister " + broker.toString());
			if (executor != null) {
				logger.info("try to shutdown ScheduledExecutorService.");
				executor.shutdown();
				try {
					executor.awaitTermination(1, TimeUnit.SECONDS);
					logger.info("ScheduledExecutorService was shutdowned successfully.");
				} catch (InterruptedException ignored) {
					logger.warn("ScheduledExecutorService was shutdowned unsuccessfully.");
				}
			}
		} catch (Exception e) {
			logger.error("unregistry fail. " + e.getMessage(), e);
		}
	}

	public String getCachedKey(Condition condition ,int begin, int limit){
		if(condition.getSort()!=null && condition.getSearchType() == SearchType.All && condition.getFilter()==null){
			return (condition.getQuery()+"|"+condition.getOperator().toString()+"|"+condition.getSort()+"|"+begin+"|"+limit).replaceAll("\\s|\t|\r|\n", "%");
		}
		return null;
	}
	@Override
	public byte[] search(byte[] req, int begin, int limit, Current current) {
		logger.info("enter search.");
		
		long startTime = System.currentTimeMillis();
		int totalDocs = 0;
		int numHit = 0;
		byte[] data = null;
		HydraResult finalResult = null;
		
		if(null==req || req.length==0){
			logger.warn("request is null or empty");
		}else{
			Condition condition = (Condition) SerializableTool.bytesToObject(req);
			if (null != condition) {
				String qStr = condition.getQuery().trim();
				if (qStr != null && !qStr.isEmpty()) {
					logger.debug("begin to get searchers by partitions");
					Map<String, String> userInfo = condition.getUserInfo();
					userInfo.put("USER_ID", String.valueOf(condition.getUserId()));
					Map<SearcherPrx, List<String>> searcher2parts = searcherPool
						.getSearcher2Partitions(userInfo);

					logger.debug("begin to build search request");

					String cacheKey = getCachedKey(condition, begin, limit);
					if(cacheKey!=null && resultCache !=null)
						finalResult = resultCache.get(cacheKey);
					
					if(finalResult==null){
						HydraRequest request = null;
						try {
							request = buildHydraRequest(condition, begin, limit);
						} catch (Exception e) {
							logger.error("build request error for condition:"+condition.toString(),e);
						}
						if(request!=null){
							logger.info("begin to search. QString: [" + condition.getOrginalQuery()
								+ "]" + " with query condition:" + condition.toString());

							List<HydraResult> results = doSearch(searcher2parts, request, begin,
								limit);
							if (results != null && !results.isEmpty()) {
								logger.debug("begin to merge result");
								finalResult = _resultMerger.merge(request, results);
								totalDocs = finalResult.getTotalDocs();
								numHit = finalResult.getNumHits();
								
								logger.info("Hit Count: " + numHit + ", "
										+ "TotalCount: " + totalDocs );
								finalResult = getNeedCount(finalResult, begin, limit);
								finalResult.setQuery(request.getQuery());
								if(this._redisClient != null){
									Map<Long, String> summaryContent = null;
									try {
										summaryContent = getSummaryContent(finalResult);
									} catch (Exception e) {
										logger.error("get content error for condition:"+condition.toString(), e);
									}
									finalResult.setContentMap(summaryContent);
								}
								if(cacheKey!=null && resultCache !=null)
									resultCache.set(cacheKey, finalResult);
							}
						}
					}
					if (finalResult!=null) {
						if (this.highlightEnable && this.hasHighlightFields && condition.isHighlight()) {
							highlight(finalResult,qStr);
						}
					} 
				}
			}
		}
		
		if(finalResult == null)
			finalResult = HydraResult.EMPTY_RESULT;
		
		data = SerializableTool.objectToBytes(finalResult);
		AVG_TOTAL_DOC.update(totalDocs);
		AVG_RETURN_DOC.update(numHit);
		SEARCH_TIMER.update(System.currentTimeMillis() - startTime,
				TimeUnit.MILLISECONDS);
		return data;
	}

	private Map<Long, String> getSummaryContent(HydraResult result)
			throws Exception {
		List<byte[]> rediskeys = new ArrayList<byte[]>();
		List<Long> uidList = new ArrayList<Long>();
		for (HydraScoreDoc hit : result.getHits()) {
			rediskeys.add(Long.toString(hit._uid).getBytes("UTF-8"));
			uidList.add(hit._uid);
		}
		List<byte[]> summaryList = _redisClient.mgetBinary(rediskeys);
		Map<Long, String> uid2SummaryMap = new HashMap<Long, String>(
				uidList.size());
		int idx = 0;
		for (byte[] summary : summaryList) {
			byte[] uncompressed = Compress.Uncompress(summary);
			if(uncompressed!=null){
				String orig = new String(uncompressed);
				uid2SummaryMap.put(uidList.get(idx), orig);
			}
			++idx;
		}
		return uid2SummaryMap;
	}

	public void highlight(HydraResult result,String qStr){
		try {
			Map<Long, JSONObject> jsonMap =  result.getContent();
			this.highlightUtil.highlight(qStr, jsonMap,
					this._highlightSummaryFields, this._needFragment);
			result.setJsonMap(jsonMap);
		} catch (Exception e) {
			logger.error("highlight error",e);
		}	
	}
	
	private void getContent(HydraResult result, boolean highlight, String qStr)
			throws Exception {
		TimeCost tc = new TimeCost();
		Map<Long, JSONObject> jsonMap =  stringMap2JsonMap(getSummaryContent(result));
		double rediscost = tc.getReset();

		if (this.highlightEnable && this.hasHighlightFields && highlight) {
			this.highlightUtil.highlight(qStr, jsonMap,
					this._highlightSummaryFields, this._needFragment);
		}
		double highlightcost = tc.getReset();

		result.setJsonMap(jsonMap);
		logger.debug("getContent. hits:" + result.getHits().length
				+ " jsonMap:" + jsonMap.size() + " rediscost:" + rediscost
				+ " highlightcost:" + highlightcost);
	}

	public Map<Long, JSONObject> stringMap2JsonMap(
			Map<Long, String> uid2SummaryMap) {
		Map<Long, JSONObject> jsons = new HashMap<Long, JSONObject>(
				uid2SummaryMap.size());
		for (Map.Entry<Long, String> e : uid2SummaryMap.entrySet()) {
			JSONObject json = null;
			try {
				json = string2Json(e.getValue());
			} catch (JSONException e1) {
				logger.error("convert summary string to jsonobject failed", e1);
			}
			jsons.put(e.getKey(), json);
		}
		return jsons;
	}

	public JSONObject string2Json(String summary) throws JSONException {
		return new JSONObject(summary);
	}

	private HydraResult getNeedCount(HydraResult finalResult, int begin,
			int limit) {
		HydraScoreDoc[] hits = finalResult.getHits();
		if (begin >= hits.length) {
			finalResult.setHits(new HydraScoreDoc[0]);
			finalResult.setNumHits(0);
			return finalResult;
		}
		int count = Math.min(begin + limit, hits.length);
		if (count > begin) {
			HydraScoreDoc[] result = new HydraScoreDoc[count - begin];
			for (int i = begin; i < count; ++i) {
				result[i - begin] = hits[i];
			}

			finalResult.setNumHits(count - begin);
			finalResult.setHits(result);
		} else {
			finalResult.setNumHits(0);
			finalResult.setHits(null);
		}

		return finalResult;

	}

	private HydraRequest buildHydraRequest(Condition condition, int begin,
			int limit) throws Exception {
		HydraRequestBuilder builder = new HydraRequestBuilder();
		logger.debug("set count: " + begin + limit);
		builder.setCount(begin + limit);
		builder.setUserId(condition.getUserId());
		builder.setUserInfo(condition.getUserInfo());
		builder.setFriendsInfoBytes(condition.getFriendsInfoBytes());
		builder.setFriendsInfo(condition.getFriendsInfo());
		String qString = condition.getOriginalQueryString();
		if(null==qString||qString.isEmpty())
			qString = condition.getQuery();
		builder.setQString(qString);
		
		Analyzer _analyzer = this.analyzerPoolHolder.getAnalyzerPool().getAnalyzer();
		try{
			QueryConditionParser _parser = (QueryConditionParser) SearchFlowFactory.createQueryParser(
				_schema, _analyzer);
		
			builder.setQuery(_parser.parseQuery(condition));
			builder.applySort(_parser.parserSort(condition));
		}catch(Exception e){
			logger.error("parser query error",e);
		}finally{
			this.analyzerPoolHolder.getAnalyzerPool().giveBackAnalyzer(_analyzer);
		}
		//builder.setQuery(_parser.parseQuery(condition));
		//builder.applySort(_parser.parserSort(condition));
		
		builder.setFilter(condition.getFilter());
		builder.setSearchType(condition.getSearchType());

		if (condition.isNeedExplain()) {
			logger.debug("show explanation");
		} else {
			logger.debug("not show explanation");
		}

		if (condition.isFillAttribute()) {
			logger.debug("fill attribute");
		} else {
			logger.debug("not fill attribute");
		}

		builder.setShowExplanation(condition.isNeedExplain());
		builder.setFillAttribute(condition.isFillAttribute());

		return builder.getRequest();
	}

	private List<HydraResult> doSearch(
			Map<SearcherPrx, List<String>> searcher2parts,
			HydraRequest request, int offset, int limit) {
		logger.debug("enter in doSearch");
		List<Job> jobs = new ArrayList<Job>();
		List<HydraResult> result = new ArrayList<HydraResult>();
		for (Map.Entry<SearcherPrx, List<String>> e : searcher2parts.entrySet()) {
			Set<Integer> parts = new HashSet<Integer>();
			for (String s : e.getValue()) {
				parts.add(Integer.decode(s));
			}

			logger.debug("send search request for partition: " + parts);
			request.setPartitions(parts);
			byte[] data = SerializableTool.objectToBytes(request);
			
			logger.debug("search from :"+e.getKey().ice_getConnection()+" for partitions:"+parts.toString());
			Job job = new Job(e.getKey(), offset, limit, data);
			if (!_pool.addJob(job)) {
				logger.warn("add job fail: " + e.getKey().ice_getConnection()
						+ "\t" + request.getQString());
				continue;
			}
			jobs.add(job);
		}
		for (Job job : jobs) {
			byte[] bytes = job.getResult(TIME_OUT);
			if (bytes != null && bytes.length > 0) {
				HydraResult res = (HydraResult) SerializableTool
						.bytesToObject(bytes);
				if (res.isEmpty()) {
					continue;
				}
				logger.debug("get numberHit: "+res.getNumHits()+"\ttotalDoc :"+res.getTotalDocs()+"\tfrom "+ job.toString());
				result.add(res);
			}
		}
		
		logger.debug("doSearch return result.");
		return result;
	}

}
