package com.renren.hydra.searcher;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.DefaultIndexReaderDecorator;

import com.renren.hydra.searcher.core.index.HydraIndexingManager;
import com.renren.hydra.searcher.core.HydraCore;
import com.renren.hydra.searcher.core.HydraZoieSystemFactory;
import com.renren.hydra.attribute.OfflineAttributeManager;
import com.renren.hydra.attribute.OnlineAttributeManager;
import com.renren.hydra.config.HydraConfParams;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.util.AnalyzerPoolHolder;
import com.renren.hydra.util.IndexFlowFactory;

public class HydraServerBuilder implements HydraConfParams {

	private static Logger logger = Logger.getLogger(HydraServerBuilder.class);
	private Comparator<String> _versionComparator;
	private HydraConfig _config;
	private int _nodeid;
	private int[] _partitions;
	private int _partitionSize;
	private Similarity _similarity;
	private ZoieConfig _zoieConfig;
	private ZoieIndexableInterpreter _interpreter;
	private Map<Integer, OnlineAttributeManager> _onlineAttrMgrMap;
	private Map<Integer, OfflineAttributeManager> _offlineAttrMgrMap;
	private HydraZoieSystemFactory<?> _zoieSystemFactory;
	private HydraCore _hydraCore;
	static final Pattern PARTITION_PATTERN = Pattern
			.compile("[\\d]+||[\\d]+-[\\d]+");

	public HydraServerBuilder(HydraConfig config) {
		_versionComparator = null;
		_config = config;
		_nodeid = -1;
		_partitions = null;
		_partitionSize = -1;
		_similarity = null;
		_zoieConfig = null;
		_onlineAttrMgrMap = new HashMap<Integer, OnlineAttributeManager>(8);
		_offlineAttrMgrMap = new HashMap<Integer, OfflineAttributeManager>(8);
		_zoieSystemFactory = null;
	}

	public HydraCore getHydraCore() {
		return _hydraCore;
	}

	public boolean init() {
		Configuration businessConfig = _config.getHydraConfig();
		if (!initVersionComparator(businessConfig)) {
			return false;
		}
		if (!initPartitions(businessConfig)) {
			return false;
		}

		if (!initOnlineAttributeMap(_partitions, businessConfig)) {
			return false;
		}

		if (!initOfflineAttributeMap(_partitions, businessConfig)) {
			return false;
		}

		if (!initZoieSystemFactory(businessConfig)) {
			return false;
		}
		
		if(!AnalyzerPoolHolder.initAnalyzerPool()){
			return false;
		}

		/*Analyzer analyzer = IndexFlowFactory.createIndexAnalyzer(_config
				.getSchema());
		if (analyzer == null) {
			logger.error("create analyzer failed");
			return false;
		} else
			logger.info("create analyzer ok");*/

		HydraIndexingManager indexManager = new HydraIndexingManager(
				_config.getSchema(), businessConfig, _versionComparator,
				null, _onlineAttrMgrMap);

		_hydraCore = new HydraCore(_nodeid, _partitions, _partitionSize,
				_config.getSchema(), _zoieSystemFactory, indexManager,
				_onlineAttrMgrMap, _offlineAttrMgrMap, _config);

		return true;
	}

	public class InitAttrTask implements Callable<Integer> {
		private Logger logger = Logger.getLogger(InitAttrTask.class);
		private int _partition;
		private Configuration _hydraConfig;
		private boolean _online;

		public InitAttrTask(int partition, Configuration hydraConfig,
				boolean online) {
			this._partition = partition;
			this._hydraConfig = hydraConfig;
			this._online = online;
		}

		@Override
		public Integer call() {
			// online attribute load
			if (_online) {
				try {
					OnlineAttributeManager onlineAttrManager = (OnlineAttributeManager) IndexFlowFactory
							.createOnlineAttributeManager(_config.getSchema(),
									_partition, _hydraConfig);

					_onlineAttrMgrMap.put(_partition, onlineAttrManager);
					logger.info("in part " + _partition
							+ ", attribute map size "
							+ onlineAttrManager.getAttrDataTable().size());
					return new Integer(1);

				} catch (Exception e) {
					logger.error(
							"fail to init Attribute Manager Map, "
									+ e.getMessage(), e);
					return new Integer(0);
				}
				// offline attribute load
			} else {
				try {
					int partId = _partition;
					String indexPath = _hydraConfig.getString(
							HydraConfParams.HYDRA_INDEX_DIRECTORY_PATH, null);
					if (indexPath == null) {
						throw new Exception(
								HydraConfParams.HYDRA_INDEX_DIRECTORY_PATH
										+ " is not confiured");
					}

					String nodeId = _hydraConfig.getString(
							HydraConfParams.NODE_ID, null);
					if (nodeId == null) {
						throw new Exception(HydraConfParams.NODE_ID
								+ " is not confiured");
					}

					String partStr = Integer.toString(partId);
					String partDir = indexPath + "/node" + nodeId + "/shard"
							+ partStr;

					logger.info("path of partition " + partId + ": " + partDir);

					OfflineAttributeManager offlineAttrManager = (OfflineAttributeManager) IndexFlowFactory
							.createOfflineAttributeManager(_config.getSchema(),
									partDir);

					_offlineAttrMgrMap.put(partId, offlineAttrManager);
					logger.info("in part " + partId
							+ ", offline attribute map size "
							+ offlineAttrManager.getAttributes().size());
					return new Integer(1);

				} catch (Exception e) {
					logger.error(
							"fail to init offline Attribute Manager Map, ", e);
					return new Integer(0);
				}

			}
		}
	}

	private boolean initOfflineAttributeMap(int[] partitions,
			Configuration hydraConfig) {

		// 创建一个执行任务的服务
		ExecutorService es = Executors.newFixedThreadPool(partitions.length);
		ArrayList<Future<Integer>> results = new ArrayList<Future<Integer>>();

		for (int i = 0; i < partitions.length; i++) {
			InitAttrTask task = new InitAttrTask(partitions[i], hydraConfig,
					false);
			results.add(es.submit(task));
		}

		for (Future<Integer> fs : results) {
			try {
				if (fs.get() == null || fs.get().intValue() != 1) {
					logger.error("fail to init offline Attribute Manager Map");
					return false;
				}
			} catch (Exception e) {
				logger.error("fail to init offline Attribute Manager Map, ", e);
				return false;
			} finally {
				es.shutdown();
			}
		}
		return true;
	}

	private boolean initOnlineAttributeMap(int[] partitions,
			Configuration hydraConfig) {
		// 创建一个执行任务的服务
		ExecutorService es = Executors.newFixedThreadPool(partitions.length);
		ArrayList<Future<Integer>> results = new ArrayList<Future<Integer>>();

		for (int i = 0; i < partitions.length; i++) {
			InitAttrTask task = new InitAttrTask(partitions[i], hydraConfig,
					true);
			results.add(es.submit(task));
		}

		for (Future<Integer> fs : results) {
			try {
				if (fs.get() == null || fs.get().intValue() != 1) {
					logger.error("fail to init Attribute Manager Map ");
					return false;
				}
			} catch (Exception e) {
				logger.error("fail to init Attribute Manager Map, ", e);
				return false;
			} finally {
				es.shutdown();
			}
		}
		return true;
	}

	private boolean initVersionComparator(Configuration config) {
		logger.info("begin to init version comparator.");

		_versionComparator = (Comparator<String>) IndexFlowFactory
				.createVesionComparator(_config.getSchema());

		if (_versionComparator != null)
			return true;

		return false;
	}

	private static int[] buildPartitions(String[] partitionArray)
			throws ConfigurationException {
		IntSet partitions = new IntOpenHashSet();
		try {
			for (int i = 0; i < partitionArray.length; ++i) {
				Matcher matcher = PARTITION_PATTERN.matcher(partitionArray[i]);
				if (!matcher.matches()) {
					throw new ConfigurationException("Invalid partition: "
							+ partitionArray[i]);
				}
				String[] partitionRange = partitionArray[i].split("-");
				int start = Integer.parseInt(partitionRange[0]);
				int end;
				if (partitionRange.length > 1) {
					end = Integer.parseInt(partitionRange[1]);
					if (end < start) {
						throw new ConfigurationException(
								"invalid partition range: " + partitionArray[i]);
					}
				} else {
					end = start;
				}

				for (int k = start; k <= end; ++k) {
					partitions.add(k);
				}
			}
		} catch (Exception e) {
			throw new ConfigurationException(
					"Error parsing partitions string: "
							+ Arrays.toString(partitionArray), e);
		}

		return partitions.toIntArray();
	}

	private boolean initPartitions(Configuration config) {
		logger.info("begin to init partitions");
		_nodeid = config.getInt(NODE_ID, -1);
		String partStr = config.getString(PARTITIONS, null);
		_partitionSize = config.getInt(PARTITION_SIZE, -1);

		if (_nodeid < 0) {
			logger.error("init partitions fail!, " + NODE_ID
					+ " is not set or should no set negative value.");
			return false;
		} else if (_partitionSize < 0) {
			logger.error("init partitions fail!, " + PARTITION_SIZE
					+ " is not set or should no set negative value.");
			return false;
		} else if (partStr == null) {
			logger.error("init partitions fail!, " + PARTITIONS
					+ " is not set.");
			return false;
		}

		String[] partitionArray = partStr.split("[,\\s]+");
		try {
			_partitions = buildPartitions(partitionArray);
			logger.info("partitions to serve: " + Arrays.toString(_partitions));
		} catch (Exception e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	private void initZoieConfig(Configuration config) {
		logger.info("begin to init zoie config");
		_zoieConfig = new ZoieConfig(_versionComparator);
		_zoieConfig.setAnalyzer(new WhitespaceAnalyzer());
		_zoieConfig.setSimilarity(_similarity);

		int batchSize = config.getInt(HYDRA_INDEX_BATCH_SIZE,
				ZoieConfig.DEFAULT_SETTING_BATCHSIZE);
		logger.info("set batch size: " + batchSize);
		_zoieConfig.setBatchSize(batchSize);

		long batchDelay = config.getLong(HYDRA_INDEX_BATCH_DELAY,
				ZoieConfig.DEFAULT_SETTING_BATCHDELAY);
		logger.info("set batch delay: " + batchDelay);
		_zoieConfig.setBatchDelay(batchDelay);

		int maxBatchSize = config.getInt(HYDRA_INDEX_BATCH_MAXSIZE,
				ZoieConfig.DEFAULT_MAX_BATCH_SIZE);
		logger.info("set max batch size: " + maxBatchSize);
		_zoieConfig.setMaxBatchSize(maxBatchSize);

		boolean isRealtime = config.getBoolean(HYDRA_INDEX_REALTIME,
				ZoieConfig.DEFAULT_SETTING_REALTIME);
		logger.info("set Realtime mode: " + isRealtime);
		_zoieConfig.setRtIndexing(isRealtime);

		long freshness = config.getLong(HYDRA_INDEX_FRESHNESS, 2000);
		logger.info("set freshness: " + freshness);
		_zoieConfig.setFreshness(freshness);
	}

	private boolean initInterpreter(Configuration config) {
		logger.info("begin to init interpreter.");
		_interpreter = (ZoieIndexableInterpreter) IndexFlowFactory
				.createJsonSchemaInterpreter(_config.getSchema());
		return true;
	}

	private boolean initZoieSystemFactory(Configuration config) {
		logger.info("begin to init ZoieSystemFactory.");

		String indexPath = config.getString(HYDRA_INDEX_DIRECTORY_PATH, null);
		if (indexPath == null) {
			logger.error("no dest index directory configured.");
			return false;
		}

		File idxDir = new File(indexPath);
		logger.info("index path: " + idxDir.getAbsolutePath());

		if (!initInterpreter(config)) {
			return false;
		}
		String dirMode = config.getString(HYDRA_INDEX_DIRECTORY_MODE, "Simple");
		DIRECTORY_MODE mode;

		if (dirMode.equalsIgnoreCase("SIMPLE")) {
			mode = DIRECTORY_MODE.SIMPLE;
		} else if (dirMode.equalsIgnoreCase("NIO")) {
			mode = DIRECTORY_MODE.NIO;
		} else if (dirMode.equalsIgnoreCase("MMAP")) {
			mode = DIRECTORY_MODE.MMAP;
		} else {
			mode = DIRECTORY_MODE.SIMPLE;
		}

		initZoieConfig(config);
		_zoieSystemFactory = new HydraZoieSystemFactory(idxDir, mode,
				_interpreter, new DefaultIndexReaderDecorator(), _zoieConfig);

		return true;
	}
	
	public Comparator<String> getVersionComparator() {
		return _versionComparator;
	}
}
