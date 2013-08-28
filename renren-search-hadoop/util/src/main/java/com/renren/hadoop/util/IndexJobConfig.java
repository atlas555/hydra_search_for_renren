package com.renren.hadoop.util;

public interface IndexJobConfig {
	
	public static final String ANALYZER_NAME = "analyzer.name";
	
	public static final String SCHEMA_FILE_URL = "schema.file.url";
	public static final String ANALYZER_DIC_DIR = "renren.analyzer.dic.dir";
	public static final String ANALYZER_DIC_NAMES = "renren.analyzer.dic.names";
	
	public static final String KAFKA_DATA_DIR = "kafka.dir";
	public static final String KAFKA_STATE_FILE = "_state.file";
	
	
	//clean data
	public static final String HISTORY_DATA_DIRS="history.dirs";
	public static final String CLEAN_DATA_OUTPUT_DIR="cleandata.output.dir";
	public static final String CLEAN_DATA_NUM_REDUCER="cleandata.num.reducer";
	
	//dedup data
	public static final String DEDUP_INPUT_DIR="dedup.input.dir";
	public static final String DEDUP_OUTPUT_DIR="dedup.output.dir";
	public static final String DEDUP_NUM_REDUCER="dedup.num.reducer";
	
	//build index
	public static final String INDEX_DATA_DIR = "index.dir";
	public static final String BUILD_INDEX_INPUT_DIR="index.input.dir";
	public static final String BUILD_INDEX_OUTPUT_DIR="index.output.dir";
	public static final String NUM_PARTITION = "num.partition";
	

	
	//build attribute
	public static final String BUILD_ATTRIBUTE_INPUT_DIR="attribute.input.dir";
	public static final String BUILD_ATTRIBUTE_OUTPUT_DIR="attribute.output.dir";
	
	public static final String DELETE_DIRS = "renren.delete";
	public static final String BAK = "renren.bak";
}
