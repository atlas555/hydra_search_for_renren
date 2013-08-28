package com.renren.hydra.searcher.core;

import java.io.File;
import java.util.Comparator;

import org.apache.log4j.Logger;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

public class HydraZoieSystemFactory<T> {
	private static Logger log = Logger.getLogger(HydraZoieSystemFactory.class);

	protected final File _idxDir;
	protected final ZoieIndexableInterpreter<T> _interpreter;
	private final IndexReaderDecorator _indexReaderDecorator;
	private final ZoieConfig _zoieConfig;
	private final DIRECTORY_MODE _dirMode;

	public HydraZoieSystemFactory(File idxDir, DIRECTORY_MODE dirMode,
			ZoieIndexableInterpreter<T> interpreter,
			IndexReaderDecorator indexReaderDecorator, ZoieConfig zoieConfig) {
		_idxDir = idxDir;
		_dirMode = dirMode;
		_interpreter = interpreter;
		_indexReaderDecorator = indexReaderDecorator;
		_zoieConfig = zoieConfig;
	}

	public ZoieSystem getZoieInstance(int nodeId, int partitionId) {
		log.info("get zoie instance");
		File partDir = getPath(nodeId, partitionId);
		if (!partDir.exists()) {
			partDir.mkdirs();
			log.info("nodeId=" + nodeId + ", partition=" + partitionId
					+ " does not exist, directory created.");
		}
		ZoieSystem zoie = new ZoieSystem(new DefaultDirectoryManager(partDir,
				_dirMode), _interpreter, _indexReaderDecorator, _zoieConfig);
		log.info("create zoie system of part " + partitionId
				+ ", MaxMergeDocs: + " + zoie.getAdminMBean().getMaxMergeDocs()
				+ ", MergeFactor: " + zoie.getAdminMBean().getMergeFactor());
		return zoie;
	}

	public static File getPath(File idxDir, int nodeId, int partitionId) {
		File nodeLevelFile = new File(idxDir, "node" + nodeId);
		return new File(nodeLevelFile, "shard" + partitionId);
	}

	// TODO: change to getDirectoryManager
	public File getPath(int nodeId, int partitionId) {
		return getPath(_idxDir, nodeId, partitionId);
	}

	public IndexReaderDecorator getDecorator() {
		return _indexReaderDecorator;
	}

	public Comparator<String> getVersionComparator() {
		return _zoieConfig.getVersionComparator();
	}
}
