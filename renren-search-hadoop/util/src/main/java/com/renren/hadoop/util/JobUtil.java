package com.renren.hadoop.util;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;

import com.chenlb.mmseg4j.analysis.SimpleAnalyzer;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.util.IndexFlowFactory;
import com.renren.search.analyzer.ThreadSafeAresAnalyzer;
import com.renren.search.util.WordSegDict;

public class JobUtil {
	public static Schema loadSchema(String schemaUrl,FileSystem fs) {
		Schema _schema = Schema.getInstance();
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setIgnoringComments(true);
			DocumentBuilder db;
			db = dbf.newDocumentBuilder();
			org.w3c.dom.Document schemaXml = db.parse(fs.open(new Path(schemaUrl)));
			schemaXml.getDocumentElement().normalize();
			_schema.initField(schemaXml);
			_schema.parserBeansAndFlow(schemaXml);
			return _schema;
		} catch (Exception e) {
			return null;
		}
	}
	
	public static Analyzer createAnalyzer(Configuration conf,Schema schema){
		Path[] caches = null;
		try {
			caches = DistributedCache.getLocalCacheFiles(conf);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		if(caches==null || caches.length==0){
			System.err.println("error no dic cache");
			return null;
		}
		
		String dicPath = caches[0].toString();
		System.out.println("local dic path is :"+dicPath);
		String analyzerName = conf.get(IndexJobConfig.ANALYZER_NAME,"mmseg");
		if(analyzerName.equals("mmseg")){
			System.out.println("usage mmsage analyzer");
			System.setProperty("mmseg.dic.path",dicPath);
			return IndexFlowFactory.createIndexAnalyzer(schema);
		}else if(analyzerName.equals("ares")){
			System.out.println("usage ares analyzer");
			System.setProperty(WordSegDict.SystemPropertyKey,dicPath);
			return IndexFlowFactory.createIndexAnalyzer(schema);
		}
		return null;
	}
	
	public static int getTaskCode(String taskId) {
		String[] temp = taskId.split("_");
		return Integer.parseInt(temp[temp.length-2]);
	}
}
