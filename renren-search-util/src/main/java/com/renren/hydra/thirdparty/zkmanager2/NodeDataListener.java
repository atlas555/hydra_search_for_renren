package com.renren.hydra.thirdparty.zkmanager2;

import org.I0Itec.zkclient.IZkDataListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NodeDataListener implements IZkDataListener {
	private static Log logger = LogFactory.getLog(NodeDataListener.class);
	private ZkCallback callback;

	public NodeDataListener(ZkCallback callback) {
		this.callback = callback;
	}

	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		callback.handleDataChange(dataPath, data);
	}

	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		callback.handleDataDeleted(dataPath);
	}
}
