package com.renren.hadoop.index.module;

import java.io.Reader;  

import org.apache.log4j.Logger;

import org.apache.lucene.analysis.Analyzer;  
import org.apache.lucene.analysis.TokenStream;  
import org.apache.lucene.analysis.WhitespaceTokenizer;  


public class ZoieAnalyzerForBuildJob extends Analyzer {	
	private static final Logger logger = Logger.getLogger(
			ZoieAnalyzerForBuildJob.class);
	
	private PayloadEncoder encoder;

	public ZoieAnalyzerForBuildJob() {
		encoder = new LongEncoder();
	}

	public TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream result = new WhitespaceTokenizer(reader);
		result = new DelimitedPayloadTokenFilter(result, '|', encoder);
		
		return result;  
	}
}	

