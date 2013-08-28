package com.renren.hydra.thirdparty.kafka;

import java.nio.ByteBuffer;
import java.util.Iterator;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;

import com.renren.hydra.index.DefaultVersionComparator;

public class HydraKafkaMessageIterator {
	private static final Logger logger = Logger.getLogger(HydraKafkaMessageIterator.class);

	private ByteBufferMessageSet[] messages;
	private int curMsgSetId;
	private Iterator<MessageAndOffset> curMsgIt;
	private ThreadLocal<byte[]> bytesFactory;
	private long[] curOffsets;
	private long[] endOffsets;

	public HydraKafkaMessageIterator() {
		long[] endOffsets = new long[HydraKafkaConsumer.max_brokers];
		DefaultVersionComparator.parseVersion(null, endOffsets);
		init(endOffsets);
	}

	public HydraKafkaMessageIterator(String endVersion) {
		long[] endOffsets = new long[HydraKafkaConsumer.max_brokers];
		DefaultVersionComparator.parseVersion(endVersion, endOffsets);
		init(endOffsets);
	}

	private void init(long[] offset) {
		bytesFactory = new ThreadLocal<byte[]>(){
			@Override
			protected byte[] initialValue() {
				return new byte[HydraKafkaConsumer.DEFAULT_MAX_MSG_SIZE];
			}
		};
		curMsgIt = null;
		endOffsets = offset;

		curOffsets = new long[HydraKafkaConsumer.max_brokers];
		for (int i = 0; i < HydraKafkaConsumer.max_brokers; i++) {
			curOffsets[i] = Long.MAX_VALUE;
		}
	}

	public void init(ByteBufferMessageSet[] messages) {
		this.messages = messages;
		curMsgIt = null;
		curMsgSetId = 0;
		if (messages == null) {
			return;
		}

		for (int i = 0; i < messages.length; i++) {
			if (messages[i] == null) {
				continue;
			}

			curMsgIt = messages[i].iterator();
			if (curMsgIt.hasNext()) {
				curMsgSetId = i;
				curOffsets[i] = 0;
				break;
			}
		}
	}

	public boolean hasNext() {
		if (curMsgIt == null) {
			return false;
		}

		if (curMsgIt.hasNext() && 
			curOffsets[curMsgSetId] < endOffsets[curMsgSetId]) {
			return true;
		}

		curMsgSetId++;
		for (; curMsgSetId < messages.length; curMsgSetId++) {
			if (messages[curMsgSetId] == null) {
				continue;
			}
	
			curMsgIt = messages[curMsgSetId].iterator();
			if (curMsgIt.hasNext()) {
				curOffsets[curMsgSetId] = 0;
				return true;
			}
		}

		return false;
	}

	public DataEvent<JSONObject> next() {
		String jsonString = null;
		String version = null;
		try {
			
			MessageAndOffset msg = null;
			long offset = 0;
			while(true) {
				msg = curMsgIt.next();
				offset = msg.offset();
				if (offset > endOffsets[curMsgSetId]) {
					curOffsets[curMsgSetId] = endOffsets[curMsgSetId];
					if (hasNext()) {
						continue;
					} else {
						return null;
					}
				} else {
					break;
				}
			}
			
			int size = msg.message().payloadSize();
			ByteBuffer byteBuffer = msg.message().payload();
			byte[] bytes = bytesFactory.get();
			byteBuffer.get(bytes, 0, size);
			jsonString = new String(bytes, 0, size, "UTF-8");
			curOffsets[curMsgSetId]	= offset;
			version = DefaultVersionComparator.toVersion(curOffsets);
			JSONObject data = new JSONObject(jsonString);		
			DataEvent<JSONObject> dataEvent = new DataEvent<JSONObject>(data, version);
			
			return dataEvent;
		}catch(JSONException e){
			logger.error("failed to init json with string: "+jsonString,e);
			return new DataEvent<JSONObject>(null, version);
		}catch (Exception e) {
			logger.error("fail to get next data, ",e);
			return null;
		}
	}
}

