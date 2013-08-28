package com.renren.hydra.thirdparty.kafka;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;

import com.renren.hydra.index.DefaultVersionComparator;
import com.renren.hydra.thirdparty.zkmanager2.NodeChildListener;
import com.renren.hydra.thirdparty.zkmanager2.ZkCallback;

public class HydraKafkaConsumer {
	private static final Logger logger = Logger.getLogger(HydraKafkaConsumer.class);

	public static final int DEFAULT_MAX_MSG_SIZE = 5*1024*1024;	
	private static String PARENT_PATH = "/brokers/ids";
	private static int timeout = 1000;
	private static int bufferSize = 10000;
	public static int max_brokers = 8;

	private long[] offsets;
	private SimpleConsumer[] simpleConsumers;
	private ByteBufferMessageSet[] results;
	private ZkClient zkClient;

	public HydraKafkaConsumer(String zkServers) {
		logger.info("zkServers:"+zkServers);
		ZkConnection zkConnection = new ZkConnection(zkServers, 10000);
		zkClient = new ZkClient(zkConnection);
		zkClient.setZkSerializer(new BytesPushThroughSerializer());
		
		simpleConsumers = new SimpleConsumer[max_brokers];
		results = new ByteBufferMessageSet[max_brokers];
		offsets = new long[max_brokers];

		init();
		
		zkClient.subscribeChildChanges(PARENT_PATH, new NodeChildListener(
				new ConsumerUpdater()));
	}

	private void init() {
		List<String> children = zkClient.getChildren(PARENT_PATH);
		Map<Integer, SimpleConsumer> consumers = 
			new HashMap<Integer, SimpleConsumer>(max_brokers);

		for (String child : children) {
			int num = Integer.parseInt(child);
			String childPath = PARENT_PATH + "/" + child;

			byte[] bytes = zkClient.readData(childPath);
			String st = new String(bytes);
			String[] sp = st.split(":");
			String url = sp[1];
			int port = Integer.parseInt(sp[2]);

			consumers.put(num, new SimpleConsumer(url, port, timeout, bufferSize));
		}
		
		for (int i = 0; i < max_brokers; i++) {
			if (consumers.containsKey(i)) {
				simpleConsumers[i] = consumers.get(i);		
			} else {
				simpleConsumers[i] = null;
			}
		}
	}
	
	public ByteBufferMessageSet[] fetch(String topic, int partId, String version) {
		DefaultVersionComparator.parseVersion(version, offsets);
		
		for (int i = 0; i < max_brokers; i++) {
			if (simpleConsumers[i] != null) {
				long offset;
				if (offsets[i] != Long.MAX_VALUE) {
					offset = offsets[i];
				} else {
					offset = 0;
				}

				FetchRequest request = new FetchRequest(topic, partId, offset,
						DEFAULT_MAX_MSG_SIZE);
				results[i] = simpleConsumers[i].fetch(request);
			} else {
				results[i] = null;
			}
		}

		return results;
	}
	
	public void close() {
		for (int i = 0; i < max_brokers; i++) {
			if (simpleConsumers[i] != null) {
				simpleConsumers[i].close();
			}
		}
	}

	private class ConsumerUpdater implements ZkCallback {
		@Override
		public void handleChildChange(String parentPath, List<String> currentChilds) {
			updateSimpleConsumers();
		}

		@Override
		public void handleDataChange(String dataPath, Object data) {
		}
		
		@Override
		public void handleDataDeleted(String dataPath) {
		}

		private void updateSimpleConsumers() {
			List<String> children = zkClient.getChildren(PARENT_PATH);
			Map<Integer, SimpleConsumer> consumers = 
				new HashMap<Integer, SimpleConsumer>();

			for (String child : children) {
				int num = Integer.parseInt(child);
				String childPath = PARENT_PATH + "/" + child;

				byte[] bytes = zkClient.readData(childPath);
				String st = new String(bytes);
				String[] sp = st.split(":");
				String url = sp[1];
				int port = Integer.parseInt(sp[2]);

				consumers.put(num, new SimpleConsumer(url, port, timeout, bufferSize));
			}
			
			for (int i = 0; i < max_brokers; i++) {
				if (consumers.containsKey(i)) {
					simpleConsumers[i] = consumers.get(i);
				} else {
					simpleConsumers[i] = null;
				}
			}
		}
	}
}
