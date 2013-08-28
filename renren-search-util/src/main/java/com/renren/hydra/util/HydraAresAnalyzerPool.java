package com.renren.hydra.util;

import org.apache.lucene.analysis.Analyzer;

import com.renren.search.analyzer.AresAnalyzer;

public class HydraAresAnalyzerPool extends HydraAnalyzerPool{
	
	public HydraAresAnalyzerPool(int num){
		super(num);
	}

	@Override
	public Analyzer crerateAnalyzer() {
		// TODO Auto-generated method stub
		return new AresAnalyzer();
	}
}
