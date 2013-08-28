package com.renren.hydra.util.loadbalance;

public class PolicyFactory {
	public static Policy createPolicy(String policy) {
		if (LoadBalanceUtil.RANDOM_POLICY.equalsIgnoreCase(policy)) {
			return new RandomPolicy();
		} else if (LoadBalanceUtil.ROUND_ROBIN_POLICY.equalsIgnoreCase(policy)) {
			return new RoundRobinPolicy();
		} else if (LoadBalanceUtil.LOAD_CAPACITY_POLICY
				.equalsIgnoreCase(policy)) {
			return new LoadCapacityPolicy();
		} else if (LoadBalanceUtil.GROUP_POLICY.equalsIgnoreCase(policy)) {
			return new GroupPolicy();
		} else {
			return new RandomPolicy();
		}
	}
}
