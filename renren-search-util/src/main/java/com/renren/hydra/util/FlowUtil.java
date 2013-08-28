package com.renren.hydra.util;

import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;

import com.renren.hydra.config.schema.Schema;

public class FlowUtil {
	public static Analyzer createAnalyzer(Schema schema, String defaultAnalyzerFlow,Map<String,String> fieldAnalyzer){
		Analyzer defaultAnalyzer = (Analyzer)ReflectUtil.createInstance(schema.getFlowNodeClass(defaultAnalyzerFlow),null,null);
		if(defaultAnalyzer==null)
			return null;
		PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer);
		
		Iterator<Map.Entry<String,String> > iter = fieldAnalyzer.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<String, String> entry = iter.next();
			String field = entry.getKey();
			String classId = entry.getValue();
			String className = schema.getClassName(classId);
			if(null!=className && !className.equals("")){
				Analyzer fAnalyzer = (Analyzer)ReflectUtil.createInstance(className,null,null);
				analyzer.addAnalyzer(field, fAnalyzer);
			}
		}
	
		return  analyzer;
	}
}
