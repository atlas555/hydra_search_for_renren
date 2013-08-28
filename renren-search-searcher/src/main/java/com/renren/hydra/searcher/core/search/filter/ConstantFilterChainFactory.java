package com.renren.hydra.searcher.core.search.filter;

import org.apache.log4j.Logger;

import com.renren.hydra.config.schema.Schema;

import com.renren.hydra.util.ReflectUtil;
import com.renren.hydra.searcher.core.HydraCore;

public class ConstantFilterChainFactory {
	private static final Logger logger = Logger
			.getLogger(ConstantFilterChainFactory.class);

	public static ConstantFilter createFilter(String filterName, HydraCore hydraCore,
			int partition) {
		logger.info("creating filter:"+filterName);
		Schema schema = hydraCore.getSchema();
		Class[] constrTypeList = new Class[] { String.class, HydraCore.class, int.class};
		Object[] constrArgList = new Object[]{filterName,hydraCore,partition};
		
		ConstantFilter f = (ConstantFilter)ReflectUtil.createInstance(schema.getFlowNodeClass(filterName), constrTypeList, constrArgList);
		return f;
	}

	public static ConstantFilterChain initFilterChain(HydraCore hydraCore,
			int partition) {
		logger.info("creating filter chain");
		Schema schema = hydraCore.getSchema();
		String[] filterNames = schema.getScoreFilterNames();
		int num = filterNames.length;
		if (0 == num) {
			return null;
		}

		ConstantFilterChain filterChain = new ConstantFilterChain(num);
		for (int i = 0; i < num; ++i) {
			ConstantFilter f = createFilter(filterNames[i], hydraCore, partition);
			if (null != f) {
				logger.info("add filter : " + filterNames[i]);
				filterChain.addFilter(f);
			}
		}
		if (filterChain.getFilterNum() != 0)
			return filterChain;
		return null;
	}
}
