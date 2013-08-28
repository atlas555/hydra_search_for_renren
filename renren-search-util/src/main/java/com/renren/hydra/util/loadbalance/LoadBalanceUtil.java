package com.renren.hydra.util.loadbalance;

import org.apache.log4j.Logger;

import com.renren.hydra.BrokerPrx;
import com.renren.hydra.SearcherPrx;
import com.renren.hydra.thirdparty.zkmanager2.Node;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.yammer.metrics.core.TimerMetric;

public class LoadBalanceUtil {
	private static final String SEARCHER_SUFFIX = " -c searcher";
	private static final String BROKER_SUFFIX = " -c broker";
	public static final String CONFIG_PREFIX = "hydra.searcher";
	public static final String CONFIG_POSTFIX = "loadbalance";
	public static final String DEFAULT_POLICY = "RANDOM";
	public static final String RANDOM_POLICY = "RANDOM";
	public static final String ROUND_ROBIN_POLICY = "ROUND_ROBIN";
	public static final String LOAD_CAPACITY_POLICY = "LOAD_CAPACITY";
	public static final String GROUP_POLICY = "GROUP";
	private static final double DIFF = 0.00001;

	public static class SendMetricsJob implements Runnable {
		private static Logger logger = Logger.getLogger(SendMetricsJob.class);
		private ZkManager zkManager;
		private String path;
		private TimerMetric metric;
		private LoadWeight weight;

		private double last = 0.0;

		public SendMetricsJob(ZkManager zkManager, String path,
				TimerMetric metric, LoadWeight weight) {
			this.zkManager = zkManager;
			this.path = path;
			this.metric = metric;
			this.weight = weight;
		}

		@Override
		public void run() {
			double QPS = metric.oneMinuteRate();
			if (Math.abs(QPS - last) > LoadBalanceUtil.DIFF) {
				//logger.debug("QPS=" + QPS + "\tlast" + last);
				Node node = zkManager.getNode(path);
				node.setQPS(QPS * weight.getWeight());
				zkManager.setNode(path, node);
				last = QPS;
			}
		}
	}

	public static interface LoadWeight {
		public double getWeight();
	}

	public static class FixedLoadWeight implements LoadWeight {
		private double weight;

		public FixedLoadWeight(double weight) {
			if (weight <= LoadBalanceUtil.DIFF) {
				this.weight = 1.0;
			} else {
				this.weight = weight;
			}
		}

		public FixedLoadWeight(Double weight) {
			if (weight == null || weight.doubleValue() <= LoadBalanceUtil.DIFF) {
				this.weight = 1.0;
			} else {
				this.weight = weight;
			}
		}

		public double getWeight() {
			return weight;
		}
	}

	private LoadBalanceUtil() {
	}

	public enum ProxyType {
		BROKER, SEARCHER
	}

	public static String brokerPrx2Id(BrokerPrx brokerPrx) {
		return brokerPrx.toString() + LoadBalanceUtil.BROKER_SUFFIX;
	}

	public static String searcherPrx2Id(SearcherPrx searcherPrx) {
		return searcherPrx.toString() + LoadBalanceUtil.SEARCHER_SUFFIX;
	}

	public static String getProxySuffix(ProxyType proxyType) {
		if (proxyType == ProxyType.BROKER) {
			return LoadBalanceUtil.BROKER_SUFFIX;
		} else if (proxyType == ProxyType.SEARCHER) {
			return LoadBalanceUtil.SEARCHER_SUFFIX;
		} else {
			return null;
		}

	}

	public static boolean isFeedback(String policy) {
		if (LoadBalanceUtil.LOAD_CAPACITY_POLICY.equalsIgnoreCase(policy))
			return true;
		return false;
	}
}
