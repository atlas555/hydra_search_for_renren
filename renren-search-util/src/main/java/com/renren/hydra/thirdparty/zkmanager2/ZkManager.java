package com.renren.hydra.thirdparty.zkmanager2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class ZkManager extends ZkConfig {
	private final static Logger logger = Logger.getLogger(ZkManager.class);

	private ZkClient client;
	private static Map<String, ZkManager> instances = new HashMap<String, ZkManager>();
	private String zkAddress;

	private ZkManager(String zkAddress) {
		this.zkAddress = zkAddress;
	}

	public void setAddress(String address) {
		this.zkAddress = address;
	}

	public void setAddress(Configuration conf) {
		this.zkAddress = conf.getString("address", null);
	}

	private synchronized boolean init() {
		if (client != null) {
			return true;
		}

		if (this.zkAddress == null) {
			logger.warn("ZK_ADDRESS should not be null, call setAddress to set it");
			return false;
		}

		ZkConnection connection = new ZkConnection(this.zkAddress);
		client = new ZkClient(connection, ZK_TIMEOUT);
		connection.getZookeeper().addAuthInfo(ACL_TYPE, ACL_INFO.getBytes());

		return true;
	}

	public static ZkManager getInstance(String zkAddress) {
		ZkManager instance = instances.get(zkAddress);
		if (instance == null) {
			synchronized (instances) {
				instance = instances.get(zkAddress);
				if (instance == null) {
					instance = new ZkManager(zkAddress);
					instances.put(zkAddress, instance);
				}
			}

		}
		return instance;
	}

	public void createNode(String parentPath, Node node) {
		createNode(parentPath, node, CreateMode.EPHEMERAL);
	}

	public boolean exists(Node node) {
		return client.exists(node.getAbsolutePath());
	}

	public boolean exists(String path) {
		return client.exists(path);
	}

	public void createNode(String parentPath, Node node, CreateMode mode) {
		if (!init()) {
			return;
		}

		if (node == null || node.isEmpty()) {
			return;
		}
		if (!client.exists(parentPath)) {
			client.createPersistent(parentPath, true);
		}
		client.create(node.getAbsolutePath(), node, mode);
	}

	public void deleteSearcherNode(Searcher node) {
		if (node == null) {
			return;
		}

		if (!init()) {
			return;
		}

		String prefix = node.getPrefix();
		String parent = node.getParentPath();
		if (!client.exists(parent)) {
			return;
		}

		List<String> children = getChildren(parent);
		for (String child : children) {
			if (child.startsWith(prefix)) {
				String path = parent + "/" + child;
				client.delete(path);
				break;
			}
		}
	}

	public void deleteNode(String path) {
		deleteNode(path, false);
	}

	public void deleteNode(String path, boolean recursive) {
		if (path == null) {
			return;
		}

		if (!init()) {
			return;
		}

		if (recursive) {
			client.deleteRecursive(path);
		} else {
			client.delete(path);
		}
	}

	public List<String> getChildren(String path) {
		List<String> children = null;
		if (path == null || path.isEmpty()) {
			children = new ArrayList<String>();
		}

		if (!init()) {
			return null;
		}

		children = client.getChildren(path);
		return children;
	}

	public Node getNode(String path) {
		if (path == null) {
			return null;
		}

		if (!init()) {
			return null;
		}

		Node node = (Node) client.readData(path);
		return node;
	}

	public void setNode(String path, Object object) {
		if (path == null) {
			return;
		}
		if (!init()) {
			return;
		}
		client.writeData(path, object);
	}

	public List<Node> getNodes(String path, String business) {
		if (!init()) {
			return null;
		}

		if (business == null || business.isEmpty()) {
			return getAllNodes(path);
		}

		if (path == null || path.isEmpty()) {
			return new ArrayList<Node>();
		}

		List<String> children = client.getChildren(path);
		List<Node> nodes = new ArrayList<Node>();
		for (String child : children) {
			Node node = null;
			try {
				node = (Node) client.readData(path + "/" + child);
			} catch (Exception e) {
				logger.error("error get node", e);
				continue;
			}
			if (node.business().equalsIgnoreCase(business)) {
				nodes.add(node);
			}
		}
		return nodes;
	}

	public List<Node> getAllNodes(String path) {
		if (path == null || path.isEmpty()) {
			return new ArrayList<Node>();
		}

		if (!init()) {
			return null;
		}

		List<String> children = client.getChildren(path);
		List<Node> nodes = new ArrayList<Node>();

		for (String child : children) {
			Node node = (Node) (client.readData(path + "/" + child));
			nodes.add(node);
		}
		return nodes;
	}

	public void subscribeDataChanges(String path, IZkDataListener listener) {
		if (!init()) {
			return;
		}

		client.subscribeDataChanges(path, listener);
	}

	public void subscribeChildChanges(String path, IZkChildListener listener) {
		if (!init()) {
			return;
		}

		client.subscribeChildChanges(path, listener);
	}

	public void subscribeStateChanges(IZkStateListener listener) {
		if (!init()) {
			return;
		}
		client.subscribeStateChanges(listener);
	}

	public static void main(String[] args) {
		// System.setProperty("isTestZookeeper", "true");
		// Broker s = new Broker("10.23.12.4", "1938", "Test");
		// ZkManager.getInstance().createNode(s);
		List<String> children = ZkManager.getInstance(ZK_LOCALHOST_ADDRESS)
				.getChildren("/search2/broker");
		System.out.println("Size: " + children.size());
		for (String node : children) {
			System.out.println(node);
		}
	}
}
