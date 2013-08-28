/**
 * 
 */
package com.renren.hydra.attribute;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import proj.zoie.impl.indexing.ZoieConfig;

import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.HydraConfParams;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.util.HydraLong;

public abstract class OnlineAttributeManager {

	private static final Logger logger = Logger
			.getLogger(OnlineAttributeManager.class);

	protected static final String VERSION_FILE_PREFIX = "attr_version";
	protected static final String ATTR_FILE_PREFIX = "attr_data";
	protected static final String DELETED_FILE_PREFIX = "delUIDs_data";
	protected static final int INIT_COUNT = 3000000;

	protected ConcurrentHashMap<HydraLong, OnlineAttributeData> _onlineAttrDataTable;
	protected int _batchSize;
	protected long _batchDelay;
	protected ThreadPoolExecutor _executor;
	protected String _indexPath;
	protected final int _partId;
	protected Timer _timer;
	protected int _curVersionId;
	protected String _curVersion;
	protected long _dataCount;
	protected Schema schema;

	public OnlineAttributeManager(int partId, Configuration hydraConfig)
			throws Exception {
		_partId = partId;
		_batchSize = hydraConfig.getInt(HydraConfParams.HYDRA_INDEX_BATCH_SIZE,
				ZoieConfig.DEFAULT_SETTING_BATCHSIZE);
		logger.info(HydraConfParams.HYDRA_INDEX_BATCH_SIZE + " : " + _batchSize);

		_batchDelay = hydraConfig.getLong(
				HydraConfParams.HYDRA_INDEX_BATCH_DELAY,
				ZoieConfig.DEFAULT_SETTING_BATCHDELAY);
		logger.info(HydraConfParams.HYDRA_INDEX_BATCH_DELAY + " : "
				+ _batchDelay);

		_indexPath = hydraConfig.getString(
				HydraConfParams.HYDRA_INDEX_DIRECTORY_PATH, null);
		if (_indexPath == null) {
			throw new Exception(HydraConfParams.HYDRA_INDEX_DIRECTORY_PATH
					+ " is not confiured");
		}
		logger.info(HydraConfParams.HYDRA_INDEX_DIRECTORY_PATH + " : "
				+ _indexPath);

		String nodeId = hydraConfig.getString(HydraConfParams.NODE_ID, null);
		if (nodeId == null) {
			throw new Exception(HydraConfParams.NODE_ID + " is not confiured");
		}
		logger.info(HydraConfParams.NODE_ID + " : " + nodeId);

		String partStr = Integer.toString(_partId);

		_indexPath = _indexPath + "/node" + nodeId + "/shard" + partStr;
		logger.info("index dir for part " + +partId + ": " + _indexPath);

		_onlineAttrDataTable = new ConcurrentHashMap<HydraLong, OnlineAttributeData>(
				INIT_COUNT);

		_curVersionId = -1;
		_curVersion = null;
		schema = HydraConfig.getInstance().getSchema();
		_executor = new ThreadPoolExecutor(1, 1, 30L, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(20),
				new ThreadPoolExecutor.DiscardOldestPolicy());

		load();

		_timer = new Timer();
		_timer.schedule(new Dumpper(this, _partId), _batchDelay, _batchDelay);
		_dataCount = 0;
	}

	public void load() throws Exception {
		logger.debug("begin to load attribute data in part: " + _partId);

		File indexDir = new File(_indexPath);
		_curVersionId = getMaxVersionId(indexDir);
		// logger.info("current version id is "+_curVersionId);

		if (_curVersionId == -1) {
			_curVersion = null;
			return;
		}

		String attrDataFilePath = _indexPath + "/" + ATTR_FILE_PREFIX + "."
				+ Integer.toString(_curVersionId);
		File attrDataFile = new File(attrDataFilePath);
		if (!attrDataFile.exists()) {
			throw new Exception(
					"load attribute data fail! cannot find data file: "
							+ attrDataFilePath);
		}

		loadData(attrDataFilePath, _onlineAttrDataTable);

		String versionFilePath = _indexPath + "/" + VERSION_FILE_PREFIX + "."
				+ Integer.toString(_curVersionId);
		_curVersion = readFromFile(versionFilePath);

		logger.info("finish loading attribute data in part: " + _partId
				+ ", data count: " + _onlineAttrDataTable.size());
	}

	public abstract void loadData(String attrDataFielPath,
			ConcurrentHashMap<HydraLong, OnlineAttributeData> _onlineAttrDataTable)
			throws Exception;
	
	public abstract void dump(int partId);

	public void add(long key, OnlineAttributeData data, String version) {
		_dataCount++;
		_onlineAttrDataTable.put(new HydraLong(key), data);
		_curVersion = version;

		if (_dataCount >= _batchSize) {
			Dumpper dumpper = new Dumpper(this, _partId);
			_executor.submit(dumpper);
			_dataCount = 0;
		}
	}

	public void delete(long key, String version) {
		if (contains(key)) {
			_onlineAttrDataTable.remove(key);
		}
		_curVersion = version;
	}

	public abstract void update(long key, OnlineAttributeData data,
			String version);

	private class VersionFileFilter implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {
			return name.startsWith(VERSION_FILE_PREFIX);
		}
	}

	protected class Dumpper extends TimerTask implements Runnable {
		private OnlineAttributeManager attrManager;
		private int partId;

		public Dumpper(OnlineAttributeManager attrManager, int partId) {
			this.attrManager = attrManager;
			this.partId = partId;
		}

		@Override
		public void run() {
			try{
				attrManager.dump(partId);
			}catch(Exception e){
				logger.error("dump failed",e);
			}
		}
	}

	public static void writeToFile(String path, String content)
			throws Exception {
		FileWriter writer = new FileWriter(path, false);
		writer.write(content);
		writer.close();
	}

	/*
	 * 优先删除version file, 只有version file 删除成功时，才删除data file
	 */
	public synchronized void delAttrDataAndVersionFiles(int versionId) {
		String versionFilePath = this.getVersionFilePath(versionId);
		String dataFilePath = this.getDataFilePath(versionId);
		File versionFile = new File(versionFilePath);
		File dataFile = new File(dataFilePath);
		
		if(versionFile.exists() && dataFile.exists()){
			if(versionFile.delete()){
				logger.info("delete version file :" + versionFilePath+" ok");
				if(dataFile.delete()){
					logger.info("delete data file :" + dataFilePath+" ok");
				}else{
					logger.warn("delete data file :" + dataFilePath+" failed");
				}
			}else{
				logger.warn("delete version file :" + versionFilePath+" failed");
			}
		}
	}

	
	public static String readFromFile(String path) throws Exception {
		File file = new File(path);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			return reader.readLine();
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	protected int getMaxVersionId(File indexDir) {
		if (!indexDir.exists()) {
			return -1;
		}

		File[] files = indexDir.listFiles(new VersionFileFilter());
		if (files == null || files.length == 0) {
			return -1;
		}

		int maxVersionId = 0;
		for (File f : files) {
			String fileName = f.getName();
			int pos = fileName.lastIndexOf('.');
			String versionIdStr = fileName.substring(pos + 1);
			int versionId = Integer.parseInt(versionIdStr);
			if (versionId > maxVersionId) {
				maxVersionId = versionId;
			}
		}

		return maxVersionId;
	}

	public ConcurrentHashMap<HydraLong, OnlineAttributeData> getAttrDataTable() {
		return _onlineAttrDataTable;
	}

	public String getDataFilePath(int versionId) {
		return _indexPath + "/" + ATTR_FILE_PREFIX + "."
				+ Integer.toString(versionId);
	}

	public String getVersionFilePath(int versionId) {
		return _indexPath + "/" + VERSION_FILE_PREFIX + "."
				+ Integer.toString(versionId);
	}
	
	public void incCurVersionId() {
		_curVersionId++;
	}

	public void setCurVersion(String version) {
		_curVersion = version;
	}

	public int getCurVersionId() {
		return _curVersionId;
	}

	public String getCurVersion() {
		return _curVersion;
	}

	public void updateVersion(String version) {
		_curVersion = version;
	}

	public boolean contains(long key) {
		HydraLong tmpKey = new HydraLong(key);
		return _onlineAttrDataTable.containsKey(tmpKey);
	}

	public OnlineAttributeData get(long key) {
		HydraLong tmpKey = new HydraLong(key);
		return _onlineAttrDataTable.get(tmpKey);
	}

	public void put(long key, OnlineAttributeData data, String version) {
		_dataCount++;
		_onlineAttrDataTable.put(new HydraLong(key), data);
		_curVersion = version;

		if (_dataCount >= _batchSize) {
			Dumpper dumpper = new Dumpper(this, _partId);
			_executor.submit(dumpper);
			_dataCount = 0;
		}
	}
}
