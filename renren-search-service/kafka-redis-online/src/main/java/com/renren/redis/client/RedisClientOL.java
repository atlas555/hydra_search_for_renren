package com.renren.redis.client;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.renren.cluster.ClusterException.ClusterConnException;
import com.renren.cluster.client.redis.RedisClusterPoolClient;

public class RedisClientOL {
	private static Logger logger = Logger.getLogger(RedisClientOL.class);
	
	private static String redisName_;
	private static String zkServers_;
	public static void main(String[] args) throws Exception {	
		String propPath = "kafka_redis_client.properties";
		if(args.length == 1) {
			propPath = args[0];
		} else {
			System.out.println("Usage: RedisClientOL <configFilePathName>");
			return;
		}
		
		org.apache.commons.configuration.Configuration config = null;
		try {
			config = new PropertiesConfiguration(propPath);
		} catch (ConfigurationException e) {
			logger.error("Can not find the config file!");
			e.printStackTrace();
			return;
		}
		
		String port = config.getString("socketPort");
		SocketThread socket_thread = null;
		try {
			int portInt = Integer.parseInt(port);
		    socket_thread = new SocketThread(portInt);
//			socket_thread.start();
		} catch(Exception e) {
			logger.error("SocketThread error:"+e.toString());	
			System.exit(1);
			return;
		}
				
		String logPath = config.getString("log4jPath", "log4j.properties");
		PropertyConfigurator.configure(logPath);
		
		redisName_ = config.getString("redisName");

		String[] zkServerArray = config.getStringArray("redisZKServers");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < zkServerArray.length; i++) {
			if (i != 0) {
				sb.append(",").append(zkServerArray[i]);
			} else {
				sb.append(zkServerArray[i]);
			}
		}
		zkServers_ = sb.toString();
		
		RedisClusterPoolClient client = null;
		try {
			client = new RedisClusterPoolClient(redisName_, zkServers_);
			client.init();
		} catch (ClusterConnException e) {
			e.printStackTrace();
			return;
		}
		
		int _partitionCount = config.getInt("partitionCount");
		int _threadCount = config.getInt("threadCount");		
		int remainder = _partitionCount % _threadCount;
		int partitionsPerThread = _partitionCount / _threadCount;
	   
		for (int i = 0; i < remainder; i++) {
			RedisThread t = new RedisThread(i * (partitionsPerThread + 1),
					partitionsPerThread + 1, config, client);
			logger.info("Thread " + i + " start");
			t.start();
		}
		for (int i = remainder; i < _threadCount; i++) {
			RedisThread t = new RedisThread(remainder * (partitionsPerThread + 1)
					+ (i - remainder) * partitionsPerThread,
					partitionsPerThread, config, client);
			logger.info("Thread " + i + " start");
			t.start();
		}
	}
}
