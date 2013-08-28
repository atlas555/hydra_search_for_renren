package com.renren.hydra.searcher.core;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import proj.zoie.api.Zoie;
import proj.zoie.impl.indexing.ZoieSystem;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OfflineAttributeManager;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeManager;
import com.renren.hydra.config.HydraConfParams;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.searcher.core.index.HydraIndexingManager;
import com.renren.hydra.searcher.core.search.filter.ConstantFilterChain;
import com.renren.hydra.searcher.core.search.filter.ConstantFilterChainFactory;
import com.renren.hydra.util.HydraLong;

public class HydraCore<D> {
	private static final Logger logger = Logger.getLogger("");
	private static final String STARTUP_FLAG_FILE = "/tmp/xce/_started";
	private HydraZoieSystemFactory<D> _zoieFactory;
	private HydraIndexingManager _indexManager;
	private final HashSet<Zoie> zoieSystems = new HashSet<Zoie>();

	private final int[] _partitions;
	private final int _id;
	private final int _partitionSize;
	private final Schema _schema;
	private final Map<Integer, ZoieSystem> _zoieSystemMap;
	private Map<Integer, OnlineAttributeManager> _onlineAttrMgrMap;
	private Map<Integer, OfflineAttributeManager> _offlineAttrMgrMap;
	private Map<Integer, ConstantFilterChain> _constFilterChainMap;
	private HydraConfig _config;

	private volatile boolean _started;

	public HydraCore(int id, int[] partitions, int partitionSize,
			Schema schema, HydraZoieSystemFactory<D> zoieSystemFactory,
			HydraIndexingManager indexManager,
			Map<Integer, OnlineAttributeManager> onlineAttrMgrMap,
			Map<Integer, OfflineAttributeManager> offlineAttrMgrMap,
			HydraConfig config) {
		_zoieFactory = zoieSystemFactory;
		_indexManager = indexManager;
		_partitions = partitions;
		_id = id;
		_partitionSize = partitionSize;
		_schema = schema;
		_onlineAttrMgrMap = onlineAttrMgrMap;
		_offlineAttrMgrMap = offlineAttrMgrMap;

		_zoieSystemMap = new HashMap<Integer, ZoieSystem>();
		_constFilterChainMap = new HashMap<Integer, ConstantFilterChain>();
		_started = false;
		_config = config;
		logger.info("id: " + _id + ", part size:" + _partitionSize);
	}

	public ConcurrentHashMap<HydraLong, OfflineAttributeData> getOffAttrDataTable(
			int partId) {
		return _offlineAttrMgrMap.get(partId).getAttributes();
	}

	public ConcurrentHashMap<HydraLong, OnlineAttributeData> getAttrDataTable(
			int partId) {
		return _onlineAttrMgrMap.get(partId).getAttrDataTable();
	}

//	//因分享搜索需要添加--梁东
	public OnlineAttributeManager getOnlineAttributeManager(int partId) {
		return _onlineAttrMgrMap.get(partId);
	}
	
	public HydraConfig getConfig() {
		return _config;
	}

	public int getNodeId() {
		return _id;
	}

	public int[] getPartitions() {
		return _partitions;
	}

	public int getPartitionSize() {
		return _partitionSize;
	}

	public Schema getSchema() {
		return _schema;
	}

	/**
	 * 1. 构建ZoieSystem，并启动，将这些zoieSystem加入到集合中 2. 初始化HydraIndexingManager，并启动
	 */
	public void start() throws Exception {
		logger.info("begin to start RexxarCore");
		if (_started)
			return;
		int delay = _config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_DELAY, 30000);
		int period = _config.getHydraConfig().getInt(
				HydraConfParams.HYDRA_FILTER_PERIOD, 86400000);
		logger.info("security manager start delay " + delay + " ms, period "
				+ period + " ms");
		for (int part : _partitions) {

			ZoieSystem zoieSystem = _zoieFactory.getZoieInstance(_id, part);

			// register ZoieSystemAdminMBean

			String[] mbeannames = zoieSystem.getStandardMBeanNames();

			if (!zoieSystems.contains(zoieSystem)) {
				zoieSystem.start();
				zoieSystems.add(zoieSystem);
			}

			_zoieSystemMap.put(part, zoieSystem);
			ConstantFilterChain filterChain = ConstantFilterChainFactory
					.initFilterChain(this, part);
			if (null != filterChain)
				_constFilterChainMap.put(part, filterChain);
		}

		logger.info("initializing index manager...");
		_indexManager.initialize(_zoieSystemMap);
		logger.info("starting index manager...");
		_indexManager.start();// 开始建索引，索引从哪里来呢？这里的dataProvider是数据库
		logger.info("index manager started...");
		_started = true;

		// finish flag
		File file = new File(STARTUP_FLAG_FILE);
		file.mkdirs();
	}

	public void shutdown() {
		if (!_started)
			return;
		logger.info("unregistering mbeans...");
		// shutdown the index manager

		logger.info("shutting down index manager...");
		_indexManager.shutdown();
		logger.info("index manager shutdown...");

		// shutdown the zoieSystems
		for (Zoie zoieSystem : zoieSystems) {
			zoieSystem.shutdown();
		}
		zoieSystems.clear();
		_started = false;
	}

	public ZoieSystem getZoieSystem(int partition) {
		return _zoieSystemMap.get(partition);
	}

	public ConstantFilterChain getConstantFilterChain(int partition) {
		return this._constFilterChainMap.get(partition);
	}
}
