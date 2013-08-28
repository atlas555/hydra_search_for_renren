package com.renren.hydra.attribute;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.config.HydraConfig;
import com.renren.hydra.thirdparty.zkmanager2.NodeDataListener;
import com.renren.hydra.thirdparty.zkmanager2.ZkCallback;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.hydra.util.HydraLong;

public abstract class OfflineAttributeManager implements ZkCallback {
	protected class Item {
		public FileStatus fileStatus = null;
		long timeStamp = 0L;

		public Item(FileStatus fs, long ts) {
			fileStatus = fs;
			timeStamp = ts;
		}
	}

	protected class Mycomparator implements Comparator {
		public int compare(Object o1, Object o2) {
			Item i1 = (Item) o1;
			Item i2 = (Item) o2;
			if (i1.timeStamp < i2.timeStamp) {
				return 0;
			} else {
				return 1;
			}
		}
	}

	protected static final Logger logger = Logger
			.getLogger(OfflineAttributeManager.class);
	protected static final String NODE_PATH = "/search2/offline_attributes";
	protected static final String OFFLINE_ATTR_FILE = "off_attr";
	protected ConcurrentHashMap<HydraLong, OfflineAttributeData> _offlineAttrDataTable;
	protected String path;
	protected long lastTimeStamp = -1;
	protected long lastAllTimeStamp = -1;
	protected Configuration conf;
	protected FileSystem fs;

	public OfflineAttributeManager(String path) throws Exception {
		_offlineAttrDataTable = new ConcurrentHashMap<HydraLong, OfflineAttributeData>(
				1000000);
		this.path = path + "/" + OFFLINE_ATTR_FILE;
		conf = new Configuration();
		fs = FileSystem.get(conf);

		load();
		ZkManager zkManager = ZkManager.getInstance(HydraConfig.getInstance().getZkProperties().getString("address"));
		String watchPath = NODE_PATH+"/"+HydraConfig.getInstance().getHydraConfig().getString("business");
		zkManager.subscribeDataChanges(watchPath,
				new NodeDataListener(this));
	}

	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds) {
	}

	@Override
	public void handleDataChange(String dataPath, Object data) {
		try {
			load();
		} catch (Exception e) {
			logger.error("load offline attribute data fail.", e);
		}
	}

	@Override
	public void handleDataDeleted(String dataPath) {
	}

	public ConcurrentHashMap<HydraLong, OfflineAttributeData> getAttributes() {
		return _offlineAttrDataTable;
	}

	public void load() throws Exception {
		logger.info("start to load offline attribute data from dir " + path);

		Path p = new Path(path);
		if (!fs.exists(p)||!fs.getFileStatus(p).isDir()) {
			logger.info("offline attribute dir " + path + " does not exist");
			return;
		}

		FileStatus[] files = fs.listStatus(p); // enum files
		ArrayList<Item> pureFiles = sortByTimeStamp(files);
		if (pureFiles == null || pureFiles.size() <= 0) {
			return;
		}

		logger.info("pure file size :"+pureFiles.size());
		long previousAllTimeStamp = lastAllTimeStamp;
		int index = findNewestAttrAllTimeStamp(pureFiles);
		logger.info("previousAllTimeStamp: " + previousAllTimeStamp
				+ ", currentAllTimeStamp: " + lastAllTimeStamp + ", index: "
				+ index);
		if (lastAllTimeStamp > previousAllTimeStamp) {
			logger.info("load from .all file");
			for (int i = index; i < pureFiles.size(); ++i) {
				Item item = pureFiles.get(i);
				String file = item.fileStatus.getPath().toString();
				logger.info("load file: " + file);
				loadData(file);
				lastTimeStamp = item.timeStamp;
			}
		} else {
			for (int i = 0; i < pureFiles.size(); ++i) {
				Item item = pureFiles.get(i);
				if (item.timeStamp <= lastTimeStamp) {
					continue;
				}

				String file = item.fileStatus.getPath().toString();
				logger.info("load file: " + file);
				loadData(file);
				lastTimeStamp = item.timeStamp;
			}
		}

		logger.info("finish load offline attribute data from dir " + path);
	}

	protected abstract void loadData(String filename) throws Exception;

	private ArrayList<Item> sortByTimeStamp(FileStatus[] files)
			throws Exception {
		if (files == null || files.length == 0) {
			return null;
		}

		ArrayList<Item> filesFiltered = new ArrayList<Item>();
		for (int i = 0; i < files.length; ++i) {
			String filePath = files[i].getPath().toString();
			long curTimeStamp = getTimeStamp(filePath);
			if (curTimeStamp == -1) {
				continue;
			}
			filesFiltered.add(new Item(files[i], curTimeStamp));
		}

		// ascending order sort by time-stamp
		if (filesFiltered.size() <= 0) {
			return null;
		}

		Collections.sort(filesFiltered, new Mycomparator());

		return filesFiltered;
	}

	private int findNewestAttrAllTimeStamp(ArrayList<Item> files)
			throws Exception {
		if (files.size() == 0) {
			return -1;
		}

		int ret = -1;
		for (int i = 0; i < files.size(); ++i) {
			String filePath = files.get(i).fileStatus.getPath().toString();
			if (filePath.indexOf(".all") != -1) {
				long curTimeStamp = getTimeStamp(filePath);
				if (curTimeStamp == -1) {
					continue;
				}

				if (curTimeStamp > lastAllTimeStamp) {
					ret = i;
					lastAllTimeStamp = curTimeStamp;
				}
			}
		}

		return ret;
	}

	private long getTimeStamp(String file) throws Exception {
		int index = file.lastIndexOf(".");
		if (index == -1) {
			return -1;
		}
		String timeStampStr = file.substring(index + 1);
		return Long.parseLong(timeStampStr);
	}

}
