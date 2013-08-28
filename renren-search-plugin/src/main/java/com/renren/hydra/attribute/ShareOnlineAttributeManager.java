package com.renren.hydra.attribute;

import java.io.File;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.json.JSONObject;


import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeManager;
import com.renren.hydra.attribute.ShareOnlineAttributeData;
import com.renren.hydra.attribute.ShareOnlineAttributeManager;
import com.renren.hydra.util.HydraLong;
import com.renren.hydra.util.SequenceFileUtil;

public class ShareOnlineAttributeManager extends OnlineAttributeManager {

	private static final Logger logger = Logger
			.getLogger(ShareOnlineAttributeManager.class);

	protected static final int INIT_DELETED_COUNT = 30000;

	protected ConcurrentHashMap<Long, Boolean> _deletedUIDs;

	public ShareOnlineAttributeManager(int partId, Configuration hydraConfig)
			throws Exception {
		super(partId, hydraConfig);
		_deletedUIDs = new ConcurrentHashMap<Long, Boolean>(INIT_DELETED_COUNT);

		loadDeletedUIDs();
	}

	@Override
	public void update(long key, OnlineAttributeData data, String version) {
		ShareOnlineAttributeData shareAttributeData = (ShareOnlineAttributeData) data;
		ShareOnlineAttributeData curData = (ShareOnlineAttributeData) _onlineAttrDataTable
				.get(key);
		curData.updateCreateTime(shareAttributeData);
		_curVersion = version;
	}

	public void deleteAll(long key, OnlineAttributeData data, String version) {
		if (contains(key)) {
			_deletedUIDs.put(key, true);
		}
		_curVersion = version;
	}

	@Override
	public void loadData(String attrDataFielPath,
			ConcurrentHashMap<HydraLong, OnlineAttributeData> onlineAttrDataTable)
			throws Exception {
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		FileSystem fs = FileSystem.get(URI.create(attrDataFielPath), conf);
		Path path = new Path(attrDataFielPath);
		logger.info("create sequence file reader for file: " + attrDataFielPath);
		SequenceFile.Reader reader = null;
		int cnt = 0;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			LongWritable key = new LongWritable();
			ShareOnlineAttributeData value = new ShareOnlineAttributeData(false);
			while (reader.next(key, value)) {
				onlineAttrDataTable.put(new HydraLong(key.get()), value);
				value = new ShareOnlineAttributeData(false);
				++cnt;
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("read num : " + cnt + " records from sequence file: "
				+ attrDataFielPath);
	}

	public void loadDeletedUIDs() {
		logger.debug("begin to load deleted uids in part: " + _partId);

		String delUIDsDataFilePath = _indexPath + "/" + DELETED_FILE_PREFIX;
		File delUIDsDataFile = new File(delUIDsDataFilePath);
		if (!delUIDsDataFile.exists()) {
			logger.error("no deletedUIDs exists", new Exception(
					"load deleted UIDs fail! cannot find data file: "
							+ delUIDsDataFilePath));
		}
		try {
			SequenceFileUtil.readDelUIDs(delUIDsDataFilePath, _deletedUIDs);
		} catch (Exception e) {
			logger.error("load deleted uids error", e);
		}

		logger.info("finish loading deleted uids in part: " + _partId
				+ ", data count: " + _deletedUIDs.size());
	}

	public ConcurrentHashMap<Long, Boolean> getDeletedUIDs() {
		return _deletedUIDs;
	}

	public boolean isUIDDeleted(long uid) {
		return _deletedUIDs.containsKey(uid);
	}

	public void deleteUID(long uid) {
		_deletedUIDs.put(uid, true);
	}

	public void recoverUID(long uid) {
		_deletedUIDs.remove(uid);
	}

	@Override
	public void dump(int partId) {
		// TODO Auto-generated method stub
		
	}
}
