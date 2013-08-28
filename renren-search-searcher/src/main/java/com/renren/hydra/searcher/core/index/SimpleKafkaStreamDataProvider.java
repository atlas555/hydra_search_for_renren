package com.renren.hydra.searcher.core.index;

import java.util.Comparator;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;

import com.renren.hydra.thirdparty.kafka.HydraKafkaConsumer;
import com.renren.hydra.thirdparty.kafka.HydraKafkaMessageIterator;

public class SimpleKafkaStreamDataProvider extends
		AbstractDataProvider<JSONObject> {
	private static Logger logger = Logger
			.getLogger(SimpleKafkaStreamDataProvider.class);
	private final String _topic;
	private String _startingVersion;
	private String _curVersion;
	private HydraKafkaMessageIterator _msgIt;
	private HydraKafkaConsumer _kafkaConsumer;
	private volatile boolean _started = false;

	public SimpleKafkaStreamDataProvider(Configuration conf,
			Comparator<String> versionComparator, int partId, String version) {
		super(conf, versionComparator, partId, version);
		super.setBatchSize(conf.getInt("batchSize"));

		_topic = conf.getString("topic");
		_curVersion = version;
		_startingVersion = version;
		_msgIt = new HydraKafkaMessageIterator();
		_kafkaConsumer = new HydraKafkaConsumer(conf.getString("zkServer"));
	}

	@Override
	public void init() {
	}

	@Override
	public void setStartingOffset(String version) {
		_startingVersion = version;
	}

	@Override
	public DataEvent<JSONObject> next() {
		if (!_started)
			return null;

		try {
			if (!_msgIt.hasNext()) {
				ByteBufferMessageSet[] messages = _kafkaConsumer.fetch(_topic,
						_partId, _curVersion);
				_msgIt.init(messages);
			}

			if (!_msgIt.hasNext()) {
				logger.debug("in part: " + _partId + ", no more data");
				return null;
			}

			DataEvent<JSONObject> dataEvent = _msgIt.next();
			_curVersion = dataEvent.getVersion();

			return dataEvent;
		} catch (Exception e) {
			logger.error("get data error for partition:"+this._partId, e);
			return null;
		}
	}

	@Override
	public void reset() {
		_curVersion = _startingVersion;
	}

	@Override
	public void start() {
		super.start();
		_started = true;
	}

	@Override
	public void stop() {
		_started = false;
		try {
			if (_kafkaConsumer != null) {
				_kafkaConsumer.close();
			}
		} finally {
			super.stop();
		}
	}

}
