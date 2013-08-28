package com.renren.kafka.backup;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
//import me.prettyprint.cassandra.serializers.StringSerializer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class KafkaToHadoop {
	private static Logger logger = Logger.getLogger(KafkaToHadoop.class);

	public static void main(String[] args) throws Exception {
		
		org.apache.commons.configuration.Configuration config = null;
		String configDir = "kafka_hadoop_client.properties";

		if (args.length != 1) {
			System.out.println("Usage:KafkaToHadoop <configFilePathName>");
			return;
		} else {
			configDir = args[0];
		}
		
		try {
			config = new PropertiesConfiguration(configDir);
		} catch (ConfigurationException e) {
			logger.error("Can not find the file!");
			e.printStackTrace();
			return;
		}
		
		String path = config.getString("log4jDir", "log4j.properties");
		PropertyConfigurator.configure(path);
		int _partitionCount = config.getInt("partitionCount", 40);
		int _threadCount = config.getInt("threadCount", 4);
		
		int remainder = _partitionCount % _threadCount;
		int partitionsPerThread = _partitionCount / _threadCount;
	   
		for (int i = 0; i < remainder; i++) {
			CThread t = new CThread(i * (partitionsPerThread + 1), partitionsPerThread + 1, config);
			logger.info("Thread " + i + " start");
			t.start();
		}
		for (int i = remainder; i < _threadCount; i++) {
			CThread t = new CThread(remainder * (partitionsPerThread + 1)
					+ (i - remainder) * partitionsPerThread, partitionsPerThread, config);
			logger.info("Thread " + i + " start");
			t.start();
		}

	}

}
