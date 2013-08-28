package com.renren.hydra.kafka.client;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;

/**
 * 人人搜索封装的kafka client端
 * 
 * @author benjamin
 * 
 */
public class ProducerClient {

	private static Logger logger = Logger.getLogger(ProducerClient.class);
	private Producer<Long, String> producer;
	private Properties props = new Properties();

	private static ProducerClient producerClient = new ProducerClient();

	public static ProducerClient getInstance() {
		return producerClient;
	}

	private ProducerClient() {
	}
	public boolean init(String file) throws FileNotFoundException {
			File f = new File(file);
			if(!f.exists())
					return false;
			InputStream in = new FileInputStream(f);
			try {
					props.load(in);
					producer = new Producer<Long, String>(new ProducerConfig(props));
			} catch (IOException e) {
					logger.error("load properties wrong", e);
			}
			return true;
	}
	
	public boolean init()  {
		System.out.println("begin init");
		InputStream in = getClass().getResourceAsStream("/resources/producer.properties");
		System.out.println("before try");
		try {
			System.out.println("in try ,before load,in="+in);
			props.load(in);
			System.out.println("in try,and in = "+in);
			producer = new Producer<Long, String>(new ProducerConfig(props));
		} catch (IOException e) {
			logger.error("load properties wrong", e);
		}
		return true;
	}

	public void sendProduceData(String topic, String data) {
		try {
			producer.send(new ProducerData<Long, String>(topic, data));
		} catch (Exception e) {
			logger.error("send data fail", e);
		}
	}

	/**
	 * 发送生产者数据
	 * 
	 * @param topic
	 *            话题
	 * @param key
	 *            id
	 * @param data
	 *            json格式的数据
	 */
	public void sendProduceDataById(String topic, long key, String data) {
		try {
			List<String> dataList = new ArrayList<String>(1);
			dataList.add(data);
			this.sendProduceListData(topic, key, dataList);
		} catch (Exception e) {
			logger.error("send data fail", e);
		}
	}

	/**
	 * 发送生产者数据
	 * 
	 * @param topic
	 *            话题
	 * @param md5
	 *            string类型的十六进制数
	 * @param data
	 *            json格式的数据
	 */
	public void sendProduceDataByMd5(String topic, String md5, String data) {
		try {
			List<String> dataList = new ArrayList<String>(1);
			dataList.add(data);
			long k = getHashValue(md5);
			this.sendProduceListData(topic, k % 1200, dataList);
		} catch (Exception e) {
			logger.error("send data fail", e);
		}
	}

	public void sendProduceListData(String topic, List<String> data) {
		try {
			producer.send(new ProducerData<Long, String>(topic, data));
		} catch (Exception e) {
			logger.error("send data fail", e);
		}
	}

	public void sendProduceListData(String topic, long key, List<String> data) {
		try {
			System.out.println("sendProduceListData,topic="+topic+",key="+key+",data="+data);
			producer.send(new ProducerData<Long, String>(topic, key, data));
		} catch (Exception e) {
			logger.error("send data fail", e);
		}
	}

	public static long getHashValue(String hashStr) {
		String low64 = hashStr.substring(16);
		long hashValue = new BigInteger(low64, 16).longValue();
		return hashValue & 0x7FFFFFFFFFFFFFFFL;
	}

}
