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


import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.util.HydraLong;
import com.renren.hydra.util.SequenceFileUtil;

public class DefaultOnlineAttributeManager extends OnlineAttributeManager {

	private static final Logger logger = Logger
			.getLogger(DefaultOnlineAttributeManager.class);

	public DefaultOnlineAttributeManager(int partId, Configuration hydraConfig)
			throws Exception {
		super(partId, hydraConfig);
	}

	@Override
	public void update(long key, OnlineAttributeData data, String version) {
		_onlineAttrDataTable.put(new HydraLong(key), data);
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
			DefaultOnlineAttributeData value = new DefaultOnlineAttributeData(schema);
			while (reader.next(key, value)) {
				onlineAttrDataTable.put(new HydraLong(key.get()), value);
				value = new DefaultOnlineAttributeData(schema);
				++cnt;
			}
		} catch (Exception e) {
			logger.error(e);
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("read num : " + cnt + " records from sequence file: "
				+ attrDataFielPath);
	}

	@Override
	public synchronized void dump(int partId) {
		if (this.getAttrDataTable().size() == 0) {
			logger.info("no data for dump");
			return;
		}
		int curVersionId = this.getCurVersionId();
		String attrFileName = this.getDataFilePath(curVersionId + 1);
		String version = this.getCurVersion();
		logger.info("in part "
						+ partId
						+ ", begin to dump attribute table with data count "
						+ this.getAttrDataTable().size() + "and versionId:"
						+ (curVersionId+1) );
		File attrFile = new File(attrFileName);
		if (attrFile.exists()) {
			logger.warn("try to delete exist attr file:" + attrFileName);
			attrFile.delete();
		}

		try {
			SequenceFileUtil.write(attrFileName, this.getAttrDataTable());
		} catch (Exception e) {
			logger.error("create sequence file : " + attrFileName
					+ " fail. " + e);
			return;
		}

		String versionFileName = this.getVersionFilePath(curVersionId + 1);
		logger.debug("version file name: " + versionFileName);
		try {
			writeToFile(versionFileName, version);
		} catch (Exception e) {
			logger.error("create version file : " + versionFileName
					+ " fail. " + e);
			return;
		}
		this.delAttrDataAndVersionFiles(curVersionId);
		this.incCurVersionId();
		logger.info("in part " + partId
						+ ", finish dumping attribute data");
	}
}
