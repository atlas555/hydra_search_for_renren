package com.renren.hydra.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SearchThreadPool {
	
	private ThreadPoolExecutor _executor;

	public SearchThreadPool() {
		_executor = new ThreadPoolExecutor(16, 32, 30L, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(20),
				new ThreadPoolExecutor.DiscardOldestPolicy());
	}
	
	/**
	 * 起一个线程
	 * @param job
	 * @return 
	 */
	public boolean addJob(final Job job) {
		Future<byte[]> future = _executor.submit(new Callable<byte[]>() {
			public byte[] call() {
				int count = job.getOffset() + job.getLimit();
				byte[] data = job.getData();
				return job.getSearcher().search(data, count);
			}
		}); 
		if (future != null) {
			job.setFuture(future);
			return true;
		}
		return false;
	}
}
