package com.renren.hydra.util;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConversionException;
import org.apache.log4j.Logger;

import com.renren.hydra.config.HydraConfig;

public class AnalyzerPoolHolder {
	private static final Logger logger = Logger.getLogger(AnalyzerPoolHolder.class);
	private HydraAnalyzerPool analyzerPool ;
	
	 private static class SingletonAnalyzerPoolHolder{ 
	     public static final AnalyzerPoolHolder INSTANCE = new AnalyzerPoolHolder();
	 }

	 public static AnalyzerPoolHolder getInstance(){
	       return SingletonAnalyzerPoolHolder.INSTANCE;
	 }
	 
	 private  AnalyzerPoolHolder(){
		 Configuration config = HydraConfig.getInstance().getHydraConfig();
		 int analyzerPoolSize = 10;
		 String analyzerPoolType = "tc";
		 if(config!=null){
			try{
				analyzerPoolSize = config.getInt("hydra.analyzer.pool.size",10);
			}catch(ConversionException e){
				logger.error(e);
				analyzerPoolSize = 10;
			}
			analyzerPoolType = config.getString("hydra.analyzer.pool.type","tc");
			logger.info("analyzer pool type is "+analyzerPoolType+" analyzer pool size is "+analyzerPoolSize);
		 }
		if(analyzerPoolType.equals("ares"))
			this.analyzerPool = new HydraAresAnalyzerPool(analyzerPoolSize);
		else
			this.analyzerPool = new HydraTCAnalyzerPool(analyzerPoolSize);
	 }
	 
	 public HydraAnalyzerPool getAnalyzerPool(){
		 return this.analyzerPool;
	 }
	 
	 public static boolean initAnalyzerPool(){
			AnalyzerPoolHolder analyzerPoolHolder = null;
			try{
				analyzerPoolHolder = AnalyzerPoolHolder.getInstance();
			}catch(Exception e){
				logger.error("create analyzer pool holder error");
				return false;
			}
			if(analyzerPoolHolder==null){
				logger.error("analyzer pool holder is null");
				return false;
			}
			return true;
		}
}
