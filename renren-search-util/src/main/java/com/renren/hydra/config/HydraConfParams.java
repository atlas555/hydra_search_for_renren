package com.renren.hydra.config;

import java.util.Comparator;

public interface HydraConfParams {
	public static final String NODE_ID = "hydra.node.id";
	public static final String PARTITIONS = "hydra.node.partitions";
	public static final String DEFAULT_FIELD = "hydra.query.default";
	public static final String PARTITION_SIZE = "hydra.index.manager.default.partitionSize";
	public static final String HYDRA_INDEX_DIRECTORY_MODE = "hydra.index.directory.mode";
	public static final String HYDRA_INDEX_DIRECTORY_PATH = "hydra.index.directory.path";

	public static final String HYDRA_INDEX_BATCH_SIZE = "hydra.index.batchSize";
	public static final String HYDRA_INDEX_BATCH_DELAY = "hydra.index.batchDelay";
	public static final String HYDRA_INDEX_BATCH_MAXSIZE = "hydra.index.maxBatchSize";
	public static final String HYDRA_INDEX_REALTIME = "hydra.index.realtime";
	public static final String HYDRA_INDEX_FRESHNESS = "hydra.index.freshness";

	public static final String HYDRA_QUERY_PARSER = "hydra.query.parser";
	public static final String HYDRA_INDEX_ANALYZER = "hydra.index.analyzer";
	public static final String HYDRA_INDEX_SIMILARITY = "hydra.index.similarity";
	public static final String HYDRA_INDEX_INTERPRETER = "hydra.index.interpreter";
	public static final String HYDRA_INDEX_MANAGER = "hydra.index.manager";
	public static final String HYDRA_INDEX_MANAGER_FILTER = "hydra.index.manager.default.filter";
	public static final String HYDRA_VERSION_COMPARATOR = "hydra.version.comparator";

	public static final String HYDRA_REDIS_NAME = "hydra.redis.name";
	public static final String HYDRA_REDIS_ZOOKEEPER_SERVER = "hydra.redis.zkServer";

	public static final String HYDRA_FILTER_DELAY = "hydra.filter.load.delay";
	public static final String HYDRA_FILTER_PERIOD = "hydra.filter.load.period";
	public static final String HYDRA_FILTER_MIN_SLOP = "hydra.filter.slop.min";
	public static final String HYDRA_FILTER_MAX_SLOP = "hydra.filter.slop.max";
	public static final String HYDRA_FILTER_SLOP_TERMLENGTH = "hydra.filter.slop.termlength";
	public static final String HYDRA_FILTER_SLOP_WORDLENGTH = "hydra.filter.slop.wordlength";

	// highlight
	public static final String HYDRA_HIGHLIGHT_SEGMENT_ENABLE = "hydra.highlight.fragment.enable";
	public static final String HYDRA_HIGHLIGHT_SEGMENT_SIZE = "hydra.highlight.fragment.size";
	public static final String HYDRA_HIGHLIGHT_TIMEOUT = "hydra.highlight.timeout";
	public static final String HYDRA_HIGHLIGHT_ENABLE = "hydra.highlight.enable";

	public static final String HYDRA_SEARCHER_LOADBALANCE = "hydra.searcher.loadbalance";
	public static final String HYDRA_LOADBALANCE_HEARTBEAT_INTVERVAL = "hydra.loadbalance.heartbeat.interval";
	public static final String HYDRA_LOADBALANCE_WEIGHT = "hydra.loadbalance.weight";
	
	public static final String HYDRA_RESULT_CACHE_ENABLE = "hydra.result.cache.enable";
	public static final String HYDRA_RESULT_CACHE_SERVER_LIST = "hydra.result.cache.serverlist";
	public static final String HYDRA_RESULT_CACHE_EXPIRE = "hydra.result.cache.expire";
	public static final String HYDRA_RESULT_CACHE_GET_TIMEOUT = "hydra.result.cache.get.timeout";

	public static final Comparator<String> DEFAULT_VERSION_STRING_COMPARATOR = new Comparator<String>() {
		@Override
		public int compare(String o1, String o2) {
			if (o1 == null && o2 == null) {
				return 0;
			}
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;
			return o1.compareTo(o2);
		}
	};

	public static final Comparator<String> DEFAULT_VERSION_LONG_COMPARATOR = new Comparator<String>() {
		@Override
		public int compare(String o1, String o2) {
			long l1, l2;
			if (o1 == null || o1.length() == 0) {
				l1 = 0L;
			} else {
				l1 = Long.parseLong(o1);
			}
			if (o2 == null || o2.length() == 0) {
				l2 = 0L;
			} else {
				l2 = Long.parseLong(o2);
			}
			return Long.valueOf(l1).compareTo(Long.valueOf(l2));
		}
	};
}
