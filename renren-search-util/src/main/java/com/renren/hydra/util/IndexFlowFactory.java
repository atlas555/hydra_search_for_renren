package com.renren.hydra.util;

import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.apache.lucene.analysis.Analyzer;

import com.renren.hydra.config.schema.Schema;

public class IndexFlowFactory {
	
	public static final String VersionComparator = "VersionComparator";
	public static final String DataProvider = "DataProvider";
	public static final String OnlineAttributeManager = "OnlineAttributeManager";
	public static final String OfflineAttributeManager = "OfflineAttributeManager";
	public static final String IndexAnalyzer = "IndexAnalyzer";
	public static final String DocumentProcessor = "DocumentProcessor";
	public static final String JsonSchemaInterpreter = "JsonSchemaInterpreter";
	
	
	public static final String DefaultVersionComparatorClass = "com.renren.hydra.index.DefaultVersionComparator";
	public static final String DefaultDataProviderClass = "com.renren.hydra.searcher.core.index.SimpleKafkaStreamDataProvider"; 
	public static final String DefaultOnlineAttributeManagerClass = "com.renren.hydra.attribute.DefaultOnlineAttributeManager";
	public static final String DefaultOfflineAttributeManagerClass = "com.renren.hydra.attribute.DefaultOfflineAttributeManager";
	public static final String DefaultIndexAnalyzerClass = "com.chenlb.mmseg4j.analysis.SimpleAnalyzer";
	public static final String DefaultDocumentProcessorClass = "com.renren.hydra.index.DefaultDocumentProcessor";
	public static final String DefaultJsonSchemaInterpreterClass = "com.renren.hydra.index.DefaultJsonSchemaInterpreter";
	
	public static Object createVesionComparator(Schema schema){
		return ReflectUtil.createInstance(schema.getFlowNodeClass(VersionComparator),null,null);
	}
	
   public static Object createJsonSchemaInterpreter(Schema schema) {
		return ReflectUtil.createInstance(schema.getFlowNodeClass(JsonSchemaInterpreter), null, null);
	}
	
	public static Object createDataProvider(Schema schema, Configuration config, Comparator versionComparator,
			int partId, String version){
		Class[] constrTypeList = new Class[] { Configuration.class,
				Comparator.class, int.class, String.class };

		Object[] constrArgList = new Object[] {config, versionComparator,
				partId, version };
		return ReflectUtil.createInstance(schema.getFlowNodeClass(DataProvider),constrTypeList,constrArgList);
	}
	
	public static Object createOnlineAttributeManager(Schema schema, int partition, Configuration config){
		Class[] constrTypeList = new Class[] { int.class,
				Configuration.class};

		Object[] constrArgList = new Object[] { partition,
				config};
		return ReflectUtil.createInstance(schema.getFlowNodeClass(OnlineAttributeManager),constrTypeList,constrArgList);
	}
	
	public static Object createOfflineAttributeManager(Schema schema, String partDir){
		Class[] constrTypeList = new Class[] { String.class };

		Object[] constrArgList = new Object[] { partDir };
		return ReflectUtil.createInstance(schema.getFlowNodeClass(OfflineAttributeManager),constrTypeList,constrArgList);
	}
	
	public static Analyzer createIndexAnalyzer(Schema schema){
		return FlowUtil.createAnalyzer(schema, IndexAnalyzer,schema.getIndexFieldAnalyzer());
	}
	
	public static Object createDocumentProcessor(Schema schema,Analyzer analyzer){
		Class[] constrTypeList = new Class[] { Analyzer.class, Schema.class };
		Object[] constrArgList = new Object[] { analyzer,schema };
		return ReflectUtil.createInstance(schema.getFlowNodeClass(DocumentProcessor),constrTypeList,constrArgList);
	}	
}
