package com.renren.hydra.searcher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import Ice.Current;

import com.renren.hydra._SearcherDisp;
import com.renren.hydra.config.HydraConfParams;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.HydraResult;
import com.renren.hydra.thirdparty.zkmanager2.Searcher;
import com.renren.hydra.thirdparty.zkmanager2.SessionExpireListener;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.hydra.util.FriendInfoUtil;
import com.renren.hydra.util.ReflectUtil;
import com.renren.hydra.util.SearchFlowFactory;
import com.renren.hydra.util.SerializableTool;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil.LoadWeight;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil.FixedLoadWeight;
import com.renren.hydra.util.loadbalance.LoadBalanceUtil.SendMetricsJob;
import com.renren.hydra.searcher.core.search.AbstractHydraCoreService;
import com.renren.hydra.searcher.core.HydraCore;
import com.renren.searchrelation.util.RedisCodec;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.TimerMetric;

public class SearcherI extends _SearcherDisp {
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(SearcherI.class);
	private static final TimerMetric SEARCH_TIMER = Metrics.newTimer(
			SearcherI.class, "SEARCH_TIMER", TimeUnit.MILLISECONDS,
			TimeUnit.SECONDS);
	private final static HistogramMetric AVG_TOTAL_DOC = Metrics.newHistogram(
			SearcherI.class, "AVG_TOTAL_DOC");
	private final static HistogramMetric AVG_RETURN_DOC = Metrics.newHistogram(
			SearcherI.class, "AVG_RETURN_DOC");
	private static ScheduledExecutorService executor;
	private static final long JOB_INTERVAL = 5;
	private AbstractHydraCoreService _coreSearcher;

	private final String _business;
	private final String _port;
	private final String _ip;
	private HydraConfig _config;
	private ZkManager zkManager;

	public SearcherI(HydraConfig config, String business, String ip, String port) {
		this._ip = ip;
		this._port = port;
		this._business = business;
		this._config = config;

		Configuration zkProperties = _config.getZkProperties();
		this.zkManager = ZkManager.getInstance(zkProperties
				.getString("address"));
	}

	public boolean init() {
		HydraServerBuilder builder = new HydraServerBuilder(_config);
		if (!builder.init()) {
			logger.error("init HydraServerBuilder fail.");
			return false;
		}

		_coreSearcher = createCoreService(builder.getHydraCore());
		if(_coreSearcher==null){
			logger.error("create core search service failed");
			return false;
		}
		return true;
	}

	public AbstractHydraCoreService createCoreService(HydraCore core){
		Class[] constrTypeList = new Class[] { HydraCore.class};
		Object[] constrArgList = new Object[]{core};
		return  (AbstractHydraCoreService) ReflectUtil.createInstance(Schema.getInstance().getFlowNodeClass(SearchFlowFactory.CoreService), constrTypeList, constrArgList);
	}
	
	public HydraCore getCore() {
		return _coreSearcher._core;
	}

	/**
	 * 向Zookeeper注册自身的信息, 提供给Broker使用
	 * 
	 * @param arg0
	 */
	public void registry(Current arg0) {
		Searcher info = new Searcher(_ip, _port, _business, getPartitions());
		try {
			zkManager.deleteSearcherNode((Searcher) info);
			Thread.sleep(2000);
			zkManager.createNode(Searcher.getParentPath(), info);
			zkManager.subscribeStateChanges(new SessionExpireListener(Searcher
					.getParentPath(), info, zkManager));
			if (LoadBalanceUtil.isFeedback(_config.getHydraConfig().getString(
					HydraConfParams.HYDRA_SEARCHER_LOADBALANCE, null))) {
				logger.info("start to init SendMetricsJob...");
				LoadWeight weight = new FixedLoadWeight(_config
						.getHydraConfig().getDouble(
								HydraConfParams.HYDRA_LOADBALANCE_WEIGHT, 1.0));
				long interval = _config.getHydraConfig().getLong(
						HydraConfParams.HYDRA_LOADBALANCE_HEARTBEAT_INTVERVAL,
						JOB_INTERVAL);
				executor = Executors.newSingleThreadScheduledExecutor();
				executor.scheduleAtFixedRate(
						new SendMetricsJob(zkManager, info.getAbsolutePath(),
								SEARCH_TIMER, weight), interval, interval,
						TimeUnit.SECONDS);
				logger.info("Info of  SendMetricsJob:{LoadWeight="
						+ weight.getClass() + ",interval=" + interval
						+ ",TimeUnit=SECONDS}");
			}
			logger.info("register node: " + info);
		} catch (Exception e) {
			logger.error("searcher registry with exception.", e);
		}
	}

	public void unregistry() {
		Searcher info = new Searcher(_ip, _port, _business, getPartitions());
		try {
			zkManager.deleteSearcherNode((Searcher) info);
			logger.info("unregister node: " + info);
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
			logger.error("searcher unregistry with exception.", e);
		}
	}

	/**
	 * 获取该服务的分区信息, 由配置文件传进来
	 * 
	 * @return
	 */
	private int[] getPartitions() {
		return this._coreSearcher._core.getPartitions();
	}

	public void start() {
		try {
			_coreSearcher._core.start();
			this.registry(null);
		} catch (Exception e) {
			logger.error("HydraCore fail to start: ", e);
		}
	}

	public void startWithoutRegistry() {
		try {
			_coreSearcher._core.start();
		} catch (Exception e) {
			logger.error("HydraCore.start exception: " + e.getMessage());
		}
	}

	
	public HydraRequest getRequest(byte[] data){
		HydraRequest request = (HydraRequest) SerializableTool.bytesToObject(data);
		Map<Integer,byte[]> _friendsInfoBytes = request.getFriendsInfoBytes();
		if(_friendsInfoBytes!=null){
			Map<Integer, Map<MutableInt,Short>> friendInfo = new HashMap<Integer, Map<MutableInt,Short>>(_friendsInfoBytes.size());
			for(Map.Entry<Integer, byte[] > e: _friendsInfoBytes.entrySet()){
				Map<MutableInt,Short> m = RedisCodec.decodeWithMutableInt(e.getValue());
				friendInfo.put(e.getKey(), m);
			}
			request.setFriendsInfoFV(friendInfo);
		}else{
			Map<Integer, Map<Integer,Short>> friendInfoTmp = request.getFriendsInfo();
			if(friendInfoTmp!=null && !friendInfoTmp.isEmpty()){
				Map<Integer,Map<MutableInt,Short>> friendInfo = FriendInfoUtil.convert2Mutable(friendInfoTmp);
				request.setFriendsInfoFV(friendInfo);
			}
		}
		return request;
	}
	
	@Override
	public byte[] search(byte[] data, int count, Current current) {
		logger.debug("enter search.");
		long startTime = System.currentTimeMillis();
		byte[] result = null;
		HydraResult hydraResult = HydraResult.EMPTY_RESULT;
		HydraRequest request = null;
		if (null != data && data.length != 0) {
			try{
				request = getRequest(data);
			}catch(Exception e){
				logger.error("get request error",e);
				request = null;
			}
			if(request!=null){
				try{
					hydraResult = _coreSearcher.execute(request);
				}catch(Exception e){
					logger.error("execute search error",e);
					hydraResult = null;
				}
				if(hydraResult == null)
					hydraResult = HydraResult.EMPTY_RESULT;
			}
		}
		
		hydraResult.setTime(System.currentTimeMillis()-startTime);
		
		try{
			result = SerializableTool.objectToBytes(hydraResult);
		}catch (Exception e) {
			logger.error("Serializable hydraResult to byte with error.", e);
		}
		
		
		AVG_TOTAL_DOC.update(hydraResult.getTotalDocs());
		AVG_RETURN_DOC.update(hydraResult.getNumHits());
		SEARCH_TIMER.update(System.currentTimeMillis()-startTime,TimeUnit.MILLISECONDS);
		logger.info("get num :" + hydraResult.getNumHits()+" hits"+" time cost:"+(System.currentTimeMillis()-startTime));
		return result;
	}
}
