package com.renren.hydra.util;

import org.apache.lucene.analysis.Analyzer;

import com.renren.search.analyzer.tc.AresTCAnalyzer;

public class HydraTCAnalyzerPool extends HydraAnalyzerPool{
	
	public HydraTCAnalyzerPool(int num){
		super(num);
	}

	@Override
	public Analyzer crerateAnalyzer() {
		// TODO Auto-generated method stub
		return new AresTCAnalyzer();
	}
	
}
