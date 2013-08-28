package com.renren.hydra.searcher.core.search;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import org.apache.lucene.index.IndexReader;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieSystem;

import com.renren.hydra.searcher.core.HydraCore;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.HydraResult;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.TimerMetric;

public abstract class AbstractHydraCoreService {
	private final static Logger logger = Logger
			.getLogger(AbstractHydraCoreService.class);

	private final static TimerMetric PARTITION_SEARCH_TIMER = Metrics.newTimer(
			AbstractHydraCoreService.class, "PARTITION_SEARCH_TIMER",
			TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
	private final static HistogramMetric PARTITION_AVG_TOTAL_DOC = Metrics
			.newHistogram(AbstractHydraCoreService.class,
					"PARTITION_AVG_TOTAL_DOC");
	private final static HistogramMetric PARTITION_AVG_RETURN_DOC = Metrics
			.newHistogram(AbstractHydraCoreService.class,
					"PARTITION_AVG_RETURN_DOC");
	protected long _timeout = 8000;
	public final HydraCore _core;
	private final ExecutorService _executorService = Executors
			.newCachedThreadPool();

	public AbstractHydraCoreService(HydraCore core) {
		_core = core;
	}

	public final HydraResult execute(final HydraRequest hydraReq) {
		logger.debug("enter execute.");

		Set<Integer> partitions = hydraReq == null ? null : hydraReq
				.getPartitions();
		logger.debug("start to execute search for partitions: " + partitions);
		// when request do not have partition info, search for partitions this
		// server host
		if (partitions == null) {
			partitions = new HashSet<Integer>();
			int[] containsPart = _core.getPartitions();
			if (containsPart != null) {
				for (int part : containsPart) {
					partitions.add(part);
				}
			}
		}
		hydraReq.setPartitionSize(_core.getPartitionSize());
		hydraReq.setPartitions(partitions);
		logger.info("search Query: " + hydraReq.getQuery());

		HydraResult finalResult;
		if (partitions != null && partitions.size() > 0) {
			final ArrayList<HydraResult> resultList = new ArrayList<HydraResult>(
					partitions.size());
			Future<HydraResult>[] futures = new Future[partitions.size() - 1];
			int i = 0;
			for (final int partition : partitions) {
				final ZoieSystem zoieSystem = _core.getZoieSystem(partition);

				if (i < partitions.size() - 1) // Search simultaneously.
				{
					try {
						futures[i] = (Future<HydraResult>) _executorService
								.submit(new Callable<HydraResult>() {
									// 针对每一个partition进行搜索
									public HydraResult call() throws Exception {
										HydraResult res = handleRequest(
												hydraReq, zoieSystem, partition);
										if (res != null) {
											PARTITION_AVG_TOTAL_DOC.update(res
													.getTotalDocs());
											PARTITION_AVG_RETURN_DOC.update(res
													.getNumHits());
										} else {
											PARTITION_AVG_TOTAL_DOC.update(0);
											PARTITION_AVG_RETURN_DOC.update(0);
										}
										return res;
									}
								});
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
				} else // Reuse current thread.
				{
					try {
						HydraResult res = handleRequest(hydraReq, zoieSystem,
								partition);
						if (res != null) {
							PARTITION_AVG_TOTAL_DOC.update(res.getTotalDocs());
							PARTITION_AVG_RETURN_DOC.update(res.getNumHits());
						} else {
							PARTITION_AVG_TOTAL_DOC.update(0);
							PARTITION_AVG_RETURN_DOC.update(0);
						}
						resultList.add(res);
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
						resultList.add(getEmptyResultInstance(e));
					}
				}

				++i;
			}

			for (i = 0; i < futures.length; ++i) {
				try {
					HydraResult res = futures[i].get(_timeout,
							TimeUnit.MILLISECONDS);
					resultList.add(res);
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					resultList.add(getEmptyResultInstance(e));
				}
			}
			try {
				logger.debug("start to merge result.");
				// 合并每个partition的结果
				finalResult = mergePartitionedResults(hydraReq, resultList);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				finalResult = getEmptyResultInstance(null);
			}
		} else {
				logger.debug("no partitions specified");
			finalResult = getEmptyResultInstance(null);
		}

		return finalResult;
	}

	private final HydraResult handleRequest(final HydraRequest hydraReq,
			final ZoieSystem zoieSystem, final int partition) throws Exception {
		logger.debug("start to handle request for partition " + partition);
		List<ZoieIndexReader<IndexReader>> readerList = null;
		try {
			readerList = zoieSystem.getIndexReaders();
			final List<ZoieIndexReader<IndexReader>> readers = readerList;
			return PARTITION_SEARCH_TIMER.time(new Callable<HydraResult>() {
				public HydraResult call() throws Exception {
					return handlePartitionedRequest(hydraReq, readers,
							partition);
				}
			});
		} finally {
			if (zoieSystem != null && readerList != null) {
				zoieSystem.returnIndexReaders(readerList);
			}
		}
	}

	public abstract HydraResult handlePartitionedRequest(HydraRequest r,
			final List<ZoieIndexReader<IndexReader>> readerList, int partition)
			throws Exception;

	public abstract HydraResult mergePartitionedResults(HydraRequest r,
			List<HydraResult> reqList);

	public HydraResult getEmptyResultInstance(Throwable error) {
		return HydraResult.EMPTY_RESULT;
	}
}
