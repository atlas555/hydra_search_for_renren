package com.renren.hydra.thirdparty.zkmanager2;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher.Event.KeeperState;

//when new session created after session expired, re-create path
public class SessionExpireListener implements IZkStateListener {

	private Node node;
	private ZkManager zkManager;
	private String parentPath;

	public SessionExpireListener(String parentPath, Node _node,
			ZkManager zkManager) {
		this.parentPath = parentPath;
		this.node = _node;
		this.zkManager = zkManager;
	}

	@Override
	public void handleNewSession() throws Exception {
		if (!zkManager.exists(node))
			zkManager.createNode(parentPath, node);
	}

	@Override
	public void handleStateChanged(KeeperState state) throws Exception {
	}

}
