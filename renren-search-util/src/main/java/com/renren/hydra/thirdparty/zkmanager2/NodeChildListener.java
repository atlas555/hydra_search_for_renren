package com.renren.hydra.thirdparty.zkmanager2;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;

public class NodeChildListener implements IZkChildListener {

	private ZkCallback callback;
	public NodeChildListener(ZkCallback callback) {
		this.callback = callback;
	}
	
	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds)
			throws Exception {
		callback.handleChildChange(parentPath, currentChilds);
	}
}
