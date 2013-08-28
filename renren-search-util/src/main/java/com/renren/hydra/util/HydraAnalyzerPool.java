package com.renren.hydra.util;

import org.apache.lucene.analysis.Analyzer;

public abstract class HydraAnalyzerPool {
	private int begin;
	private int end;
	private Analyzer[] analyzerPool;
	private int size;
	
	public HydraAnalyzerPool(int num){
		analyzerPool = new Analyzer[num + 1];
		for (int i = 1; i < num + 1; i++) {
			analyzerPool[i] = crerateAnalyzer();
		}
        size = num + 1;
        begin = 1;
        end = 0;
	}
	
	public abstract Analyzer crerateAnalyzer();
	
	public synchronized Analyzer getAnalyzer(){
		while (begin == end) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		int index = begin % size;
		begin = (begin + 1) % size;
		return analyzerPool[index];
	}
	
	public synchronized void giveBackAnalyzer(Analyzer analyzer){
		if(Math.abs(begin - end) == 1) {
			return;
		}
		analyzerPool[end] = analyzer;
		end = (end + 1) % size;
		notify();
	}
}
