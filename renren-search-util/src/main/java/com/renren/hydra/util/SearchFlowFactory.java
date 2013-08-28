package com.renren.hydra.util;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.filter.FilterInfo;
import com.renren.hydra.search.filter.SearchFilter;
import com.renren.hydra.search.pretreat.ISearchPretreater;


public class SearchFlowFactory {
	
	public static final String QueryParser = "QueryParser";
	public static final String SearchAnalyzer = "SearchAnalyzer";
	public static final String HighlightAnalyzer = "HighlightAnalyzer";
	public static final String Similarity = "Similarity";
	public static final String FilterChain = "ConstantFilterChain";
	public static final String CoreService = "CoreService";
	public static final String SearchPretreater="SearchPretreater";
	
	public static final String DefaultQueryParserClass = "com.renren.hydra.search.parser.DefaultQueryConditionParser";
	public static final String DefaultSearchAnalyzerClass = "com.chenlb.mmseg4j.analysis.SimpleAnalyzer";
	public static final String DefaultHighlightAnalyzerClass = "com.renren.search.analyzer.standard.AresStandardAnalyzer";
	public static final String DefailtSimilarityClass = "com.renren.hydra.searcher.core.search.HydraDefaultSimilarity";
	public static final String CoreServiceClass = "com.renren.hydra.searcher.core.search.CoreHydraServiceImpl";
	public static final String SearchPretreaterClass = "com.renren.hydra.search.pretreat.DefaultSearchPretreater";
	
	public static Object createQueryParser(Schema schema,Analyzer analyzer){
		Class[] constrTypeList = new Class[] { Analyzer.class, Schema.class };
		Object[] constrArgList = new Object[]{analyzer,schema};
		return  ReflectUtil.createInstance(schema.getFlowNodeClass(QueryParser), constrTypeList, constrArgList);
	}
	
	public static Analyzer createSearchAnalyzer(Schema schema){
		return FlowUtil.createAnalyzer(schema, SearchAnalyzer,schema.getSearchFieldAnalyzer());
	}
	
	public static Analyzer createHighlightAnalyzer(Schema schema){
		return (Analyzer) ReflectUtil.createInstance(schema.getFlowNodeClass(HighlightAnalyzer), null, null);
	}
	
	public static Object createSimilarity(Schema schema, Query query, boolean showExplain){
		Class[] constrTypeList = new Class[] { Schema.class,
				Query.class, boolean.class};
		Object[] constrArgList = new Object[] { schema,query,showExplain};
		return  ReflectUtil.createInstance(schema.getFlowNodeClass(Similarity), constrTypeList, constrArgList);
	}
	
	public static SearchFilter createSearchFilter(Schema schema,FilterInfo filterInfo){
		Class[] constrTypeList = new Class[] {String.class};
		Object[] constrArgList = new Object[] {filterInfo.getFilterCondition()};
		SearchFilter searchFilter = (SearchFilter) ReflectUtil.createInstance(schema.getClassName(filterInfo.getFilterName()), constrTypeList, constrArgList);
		return searchFilter;
	}
	
	public static Object createFilterChain(Schema schema, Class[] constrTypeList, Object[] constrArgList){
		return null;
	}
	
	public static ISearchPretreater createSearchPretreater(Schema schema){
		return  (ISearchPretreater) ReflectUtil.createInstance(schema.getFlowNodeClass(SearchPretreater), null, null);
	}
	
	
}
