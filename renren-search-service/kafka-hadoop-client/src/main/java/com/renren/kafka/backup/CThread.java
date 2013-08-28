package com.renren.kafka.backup;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;

import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.thirdparty.kafka.HydraKafkaConsumer;
import com.renren.hydra.thirdparty.kafka.HydraKafkaMessageIterator;
import com.renren.hydra.util.IndexFlowFactory;

import org.w3c.dom.*;
import java.lang.Thread;

public class CThread extends Thread {
	private Logger logger = Logger.getLogger(CThread.class);
	private static String zkServers_;
	private HydraKafkaConsumer[] _rkc;
	private HydraKafkaMessageIterator[] _rkmessageIt;
	private static String dataDir_;
	private static int versionCheckPoint_;
	private static String topic_;
	// 写到了第几个文件，重启程序时，读取hdfs数据初始化
	private int[] fileth;
	private String[] _versions;
	private int _beginPartition;
	private int _partitionLimit;
	private int[] _dumpCounts;
	private FileSystem local;
	private SequenceFile.Writer[] outs;
	private org.apache.hadoop.conf.Configuration _hadoopConf;
	private String uidField_;
	private DocumentProcessor documentProcessor_;

	public CThread(int begin, int limit, Configuration conf) {
		logger.info("construct thread managing partions of " + begin + " to " + (begin + limit));
		this._beginPartition = begin;
		this._partitionLimit = limit;
		this._versions = new String[limit];
		this.fileth = new int[limit];
		_dumpCounts = new int[limit];
		
		topic_ = conf.getString("topic");

		parseSchema(conf.getString("SchemaFile"));
		
		versionCheckPoint_ = conf.getInt("versionCheckPoint", 400000);
		String[] zkServerArray = conf.getStringArray("zkServers");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < zkServerArray.length; i++) {
			if (i != 0) {
				sb.append(",").append(zkServerArray[i]);
			} else {
				sb.append(zkServerArray[i]);
			}
		}
		zkServers_ = sb.toString();
		logger.info(zkServers_);

		try {
			_hadoopConf = new org.apache.hadoop.conf.Configuration();
			dataDir_ = conf.getString("dataDir");
			local = FileSystem.getLocal(_hadoopConf);
			this.outs = new SequenceFile.Writer[limit];
		} catch (IOException e) {
			logger.info("IOException");
			e.printStackTrace();
		}
		this._rkc = new HydraKafkaConsumer[limit];
		this._rkmessageIt = new HydraKafkaMessageIterator[limit];
		for (int i = 0; i < limit; i++) {
			_rkc[i] = new HydraKafkaConsumer(zkServers_);
			_rkc[i].max_brokers = 2;
			_rkmessageIt[i] = new HydraKafkaMessageIterator();
		}

	}

	@SuppressWarnings("deprecation")
	public void init() throws IOException {		
		for (int i = _beginPartition; i < _beginPartition + _partitionLimit; i++) {
			_dumpCounts[i % _partitionLimit] = 0;
			String childDir = dataDir_ + "/part_" + i;
			logger.info(childDir);
			File partitionFileDir = new File(childDir);
			int versionNum = 0;
			//存在partition目录
			if(partitionFileDir.exists()) {
				File[] files = partitionFileDir.listFiles();
				// 获取当前version文件
				for (int j = 0; j < files.length; j++) {
					if (files[j].toString().contains("version") && (!files[j].toString().contains("crc")))
						versionNum++;
				}
				fileth[i % _partitionLimit] = versionNum;
				if(versionNum != 0) {
					FSDataInputStream in = local.open(new Path(childDir + "/version." + (versionNum-1)));
					_versions[i % _partitionLimit] = in.readLine().trim();
					in.close();
				}
			} else {
				partitionFileDir.mkdir();
				fileth[i % _partitionLimit] = versionNum;
			}
			// 打开数据接收文件
			Path curFile = new Path(childDir, String.valueOf(fileth[i % _partitionLimit]));
			outs[i % _partitionLimit] = SequenceFile.createWriter(local,
					_hadoopConf, curFile, LongWritable.class, Text.class, CompressionType.BLOCK);
		}
	}

	public synchronized void run() {
		try {
			init();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
              
        boolean hasMoreData = false;
		while (true) {
            hasMoreData = false;
			for (int i = _beginPartition; i < _beginPartition + _partitionLimit; i++) {
				logger.info("partition: " + i);
				logger.info("partition: " + i + " version: " + _versions[i % _partitionLimit]);
				ByteBufferMessageSet[] messageSets = _rkc[i % _partitionLimit]
						.fetch(topic_, i, _versions[i % _partitionLimit]);
				_rkmessageIt[i % _partitionLimit].init(messageSets);
                                
				if(_rkmessageIt[i % _partitionLimit].hasNext())
					hasMoreData = true;

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
					//构造SequenceFile的key和value
					long id = documentProcessor_.getUid(json);
					try {
						json.put("version", new_version);
					} catch (JSONException e) {
						logger.error("JSONException");
						e.printStackTrace();
					}
					try {
						outs[i % _partitionLimit].append(new LongWritable(id), new Text(json.toString()));
					} catch (IOException e) {
						logger.error("IOException");
						e.printStackTrace();
					}

					// 保存中间version，防止重启需要重新开始读取
					_dumpCounts[i % _partitionLimit]++;
					if (_dumpCounts[i % _partitionLimit] == versionCheckPoint_) {
                                                logger.info("dump to file, record num:"+_dumpCounts[i%_partitionLimit]+" for partition " +i);	
						Path versionFile = new Path(dataDir_ + "/part_" + i, "version." + fileth[i % _partitionLimit]);
						FSDataOutputStream out;
						try {
							out = local.create(versionFile, true);
							out.write(_versions[i % _partitionLimit].getBytes("UTF-8"));
							out.close();
						} catch (IOException e) {
							logger.error("IOException");
							e.printStackTrace();
						}
						// 关闭旧文件，打开新的接收文件
						fileth[i % _partitionLimit]++;
						logger.info("Begin a new file....");
						try {
							outs[i % _partitionLimit].close();
							Path curFile = new Path(dataDir_ + "/part_" + i, String.valueOf(fileth[i % _partitionLimit]));
							Thread.sleep(1000);
							outs[i % _partitionLimit] = SequenceFile.createWriter(local,
									_hadoopConf, curFile, LongWritable.class, Text.class, CompressionType.BLOCK);
						} catch (IOException e) {
							logger.info("IOException");
							e.printStackTrace();
						} catch (InterruptedException e) {
							logger.info("InterruptedException");
							e.printStackTrace();
						}
						_dumpCounts[i % _partitionLimit] = 0;
					}
				} //End of while
			}
			if(!hasMoreData){
				logger.info("no more data now,sleeping half hours");
                                try{
					Thread.sleep(1800000);
				}catch(Exception e){
					logger.error(e);
				}
			}
		} //End of while(true)
	}

	private void parseSchema(String schemaFilePathName) {
		try {
            		Schema schema = Schema.getInstance();
            		schema.initSchema(schemaFilePathName);
            		uidField_ = schema.getUidField();
            		documentProcessor_ = (DocumentProcessor)IndexFlowFactory.createDocumentProcessor(schema, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
