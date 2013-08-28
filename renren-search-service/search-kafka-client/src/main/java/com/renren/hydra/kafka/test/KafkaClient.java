package com.renren.hydra.kafka.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONException;
import org.json.JSONObject;

import com.renren.hydra.kafka.client.ProducerClient;

public class KafkaClient {
	private static Logger logger = Logger.getLogger(KafkaClient.class);
	private static int partition=0;
	private static String topic ="";
	private static ProducerClient producerClient = null;
	private static String idField="goodsId";
	
	public static void sendData(Long id, String value){
		try {
			producerClient
					.sendProduceDataById(topic,id % partition, value);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public static void sendData(JSONObject obj) throws JSONException{
		sendData(obj.optLong(idField),obj.toString());
	}

	
	public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException{

		if(args.length<6){
			System.out.println("usage, KafkaClient topicname partitionnum propertiesfile  logconfigfile jsontxtfile idfield");
			System.exit(-1);
		}
		topic =args[0];
		partition = Integer.parseInt(args[1]);
		
		String propertyFile=args[2];
		producerClient = ProducerClient.getInstance();
		producerClient.init(propertyFile);

	
		
		String logfile = args[3];
		PropertyConfigurator.configure(logfile);
		
		
		String dataFile=args[4];
		File file = new File(dataFile);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		idField = args[5];
		
		String line = "";
		int count = 0;
		
		System.out.println("sending data with topic:"+topic);
	
		
		while(true){
			line = bufferedReader.readLine();
			if(line==null)
				break;
			
			try {
				sendData(new JSONObject(line));
			} catch (JSONException e) {
				e.printStackTrace();
			}
			++count;
			if(count%10000==0){
				System.out.println(line);
				System.out.println("processed "+count+" records");
			}
		}
		System.out.println("record num :"+count);
		//Thread.sleep(2000);
		//System.exit(0);
	}
}
