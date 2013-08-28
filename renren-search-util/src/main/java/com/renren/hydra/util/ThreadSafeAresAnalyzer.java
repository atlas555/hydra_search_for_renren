package com.renren.hydra.util;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

import com.renren.search.analyzer.AresAnalyzer;

public class ThreadSafeAresAnalyzer extends Analyzer{
	private final static Logger logger = Logger.getLogger(ThreadSafeAresAnalyzer.class);
	private Map<Thread,AresAnalyzer> hardRefs = null;
	private static int PURGE_MULTIPLIER = 10;
	private final AtomicInteger countUntilPurge = new AtomicInteger(PURGE_MULTIPLIER);
	
	public ThreadSafeAresAnalyzer(){
		this.hardRefs = new HashMap<Thread,AresAnalyzer>();
	}
	
	public Analyzer get(){
		Thread t = Thread.currentThread();
		AresAnalyzer analyzer = hardRefs.get(t);
		if(analyzer==null){
			 synchronized(hardRefs) {
				  analyzer = new AresAnalyzer();
			      hardRefs.put(Thread.currentThread(), analyzer);
			      maybePurge();
			    }
		}
		return analyzer;
	}
	
	private void maybePurge() {
	    if (countUntilPurge.getAndDecrement() == 0) {
	      purge();
	    }
	  }
	
	public void purge() {
	    synchronized(hardRefs) {
	      int stillAliveCount = 0;
	      for (Iterator<Thread> it = hardRefs.keySet().iterator(); it.hasNext();) {
	        final Thread thread = it.next();
	        if (!thread.isAlive()) {
	          AresAnalyzer o = hardRefs.get(thread);
	          try{
	        	  o.close();
	        	  o=null;
	          } catch (Exception e) {
	        	  logger.error("close analyzer error",e);
	          }
	          it.remove();
	        } else {
	          stillAliveCount++;
	        }
	      }
	      int nextCount = (1+stillAliveCount) * PURGE_MULTIPLIER;
	      if (nextCount >= 100000) {
	        nextCount = 100000;
	      }
	      countUntilPurge.set(nextCount);
	    }
	  }
	
	
	@Override
	public TokenStream tokenStream(String fieldName, Reader in) {
		return this.get().tokenStream(fieldName, in);
	}

	@Override
	public TokenStream reusableTokenStream(String fieldName, Reader in)
			throws IOException {
		return this.get().tokenStream(fieldName, in);
	}
	
	@Override
	public void close(){
		for (Iterator<Thread> it = hardRefs.keySet().iterator(); it.hasNext();) {
	        final Thread thread = it.next();
	        AresAnalyzer o = hardRefs.get(thread);
	        o.close();
	        it.remove();
		}
		this.hardRefs = null;
	}
}
