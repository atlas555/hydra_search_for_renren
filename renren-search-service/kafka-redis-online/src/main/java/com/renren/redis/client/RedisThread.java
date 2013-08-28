package com.renren.redis.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;

import proj.zoie.api.DataConsumer.DataEvent;

import com.renren.cluster.client.redis.RedisClusterPoolClient;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.thirdparty.kafka.HydraKafkaConsumer;
import com.renren.hydra.thirdparty.kafka.HydraKafkaMessageIterator;
import com.renren.hydra.util.IndexFlowFactory;
import com.renren.hydra.index.DocumentStateParser;

public class RedisThread extends Thread {
	private Logger logger = Logger.getLogger(RedisThread.class);
	private String zkServers_;
	private HydraKafkaConsumer[] rkafkaConsumers_;
	private HydraKafkaMessageIterator[] _rkmessageIt;
	private String[] _versions;
	private int _beginPartition;
	private int _partitionLimit;
	private int[] _dumpCounts;
	private FileOutputStream _fos;
	private DataOutputStream _dos;
	private Configuration _config;
	private RedisClusterPoolClient redisClient_;
	private DocumentProcessor documentProcessor_;
	private String uidField_;
	private String deleteField_;
	private Set<String> summaryFields_ = new HashSet<String>();
	private int versionCheckPoint_;
	private String versionFileDir_;

	public RedisThread(int begin, int limit, Configuration config, RedisClusterPoolClient client) {
		this._beginPartition = begin;
		this._partitionLimit = limit;
		this._config = config;
		this._versions = new String[limit];
		_dumpCounts = new int[limit];
		redisClient_ = client;
		logger.info("construct thread managing partions of " + begin + " to " + (begin + limit));
		versionCheckPoint_ = config.getInt("versionCheckPoint", 1000);
		versionFileDir_ = config.getString("versionFileDir", "version");
		//读取Schema文件
		parseSchema(config.getString("SchemaFile"));

		String[] zkServerArray = config.getStringArray("kafkaZKServers");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < zkServerArray.length; i++) {
			if (i != 0) {
				sb.append(",").append(zkServerArray[i]);
			} else {
				sb.append(zkServerArray[i]);
			}
		}
		// 初始化HydraKafkaConsumer
		zkServers_ = sb.toString();
		this.rkafkaConsumers_ = new HydraKafkaConsumer[limit];
		this._rkmessageIt = new HydraKafkaMessageIterator[limit];
		for (int i = 0; i < limit; i++) {
			rkafkaConsumers_[i] = new HydraKafkaConsumer(zkServers_);
			rkafkaConsumers_[i].max_brokers = 2;
			_rkmessageIt[i] = new HydraKafkaMessageIterator();
		}
	}

	public void init() throws IOException {
		for (int i = _beginPartition; i < _beginPartition + _partitionLimit; i++) {
			_dumpCounts[i % _partitionLimit] = 0;
			File verDir = new File(versionFileDir_);
			if (!verDir.exists()) {
				verDir.mkdirs();
			} else {
				File f = new File(versionFileDir_ + "/version" + i);
				if (f.exists()) {
					FileInputStream fis = new FileInputStream(versionFileDir_ + "/version" + i);
					DataInputStream dis = new DataInputStream(fis);
					this._versions[i % _partitionLimit] = dis.readLine().trim();
					logger.info(_versions[i % _partitionLimit]);
					try {
						dis.close();
						fis.close();
					} catch (FileNotFoundException e) {
						logger.warn("FileNotFoundException happened!");
						e.printStackTrace();
					} catch (IOException e) {
						logger.warn("IOException happened!");
						e.printStackTrace();
					}
				}
			}
		}
	}

	public synchronized void run() {
		try {
			init();
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (true) {
			for (int i = _beginPartition; i < _beginPartition + _partitionLimit; i++) {
				//logger.info("partition: " + i + " version: " + _versions[i % _partitionLimit]);
				ByteBufferMessageSet[] messageSets = null;
				try {					
					messageSets = rkafkaConsumers_[i % _partitionLimit]
							.fetch(_config.getString("topic"), i, _versions[i % _partitionLimit]);
					_rkmessageIt[i % _partitionLimit].init(messageSets);										

					while (_rkmessageIt[i % _partitionLimit].hasNext()) {
						DataEvent<JSONObject> de = _rkmessageIt[i % _partitionLimit].next();
						JSONObject json = null;
						try {
						    json = de.getData();
						} catch (Exception e) {
	                        json = null;
						}
						if (null == json) {
							continue;
						}			
						String new_version = de.getVersion();
						logger.info("partition: " + i + "new_version:" + new_version);
						_versions[i % _partitionLimit] = new_version;		

						// 保存中间offset，防止重启需要重新开始读取
						_dumpCounts[i % _partitionLimit]++;
						if (_dumpCounts[i % _partitionLimit] == versionCheckPoint_) {
							dumpversion(i);
							_dumpCounts[i % _partitionLimit] = 0;
						}

						try {
							long key = documentProcessor_.getUid(json);
									
							if (json.optBoolean(deleteField_, false)) {
								logger.info("deleted item of key:" + key);
								continue;
							} else {
								JSONObject formattedJson = format(json); 
								if (formattedJson == JSONObject.NULL) {
									continue;
								}
								redisClient_.set(Long.toString(key).getBytes("UTF-8"), 
										Util.Compress(formattedJson.toString().getBytes("UTF-8")));
								logger.info("key: " + key);
							}
						}  catch (UnsupportedEncodingException e) {					
							logger.error("UnsupportedEncodingException happened!");
							e.printStackTrace();
						} catch (Exception e) {
							logger.error("Exception happened!");
							e.printStackTrace();
						}

					}									
				} catch(Exception e) {
					logger.error("fetch error:"+e.toString());
					e.printStackTrace();
				}
								

			}
		}
	}

	public void dumpversion(int partition) {
		try {
			_fos = new FileOutputStream(versionFileDir_ + "/version" + partition);
			_dos = new DataOutputStream(_fos);
			_dos.writeBytes(_versions[partition % _partitionLimit]);
			_dos.close();
			_fos.close();
		} catch (FileNotFoundException e) {
			logger.warn("FileNotFoundException happened!");
			e.printStackTrace();
		} catch (IOException e) {
			logger.warn("IOException happened!");
			e.printStackTrace();
		}
	}
	
	private JSONObject format(JSONObject json) {
		JSONObject newJson = new JSONObject();
		try {
			for (String field : summaryFields_) {
				newJson.put(field, json.opt(field));
			}
			return newJson;
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private void parseSchema(String schemaFilePathName) {
		try {
			Schema schema = Schema.getInstance();
			schema.initSchema(schemaFilePathName);
			uidField_ = schema.getUidField();
			deleteField_ = DocumentStateParser.DELETE_FIELD;
			documentProcessor_ = (DocumentProcessor)IndexFlowFactory.
					createDocumentProcessor(schema, null);
			Set<String> highlightSet = null;
			try {
				highlightSet = schema.getHighlightSummaryFields();
			} catch (NullPointerException e ) {

			}
			if (highlightSet != null && !highlightSet.isEmpty()) {
				summaryFields_.addAll(highlightSet);
			}
			
			Set<String> noHighlightSet = null;
			try {
				noHighlightSet = schema.getNoHighlightSummaryFields();
			} catch (NullPointerException e ) {

			}
			if (noHighlightSet != null && !noHighlightSet.isEmpty()) {
				summaryFields_.addAll(noHighlightSet);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
