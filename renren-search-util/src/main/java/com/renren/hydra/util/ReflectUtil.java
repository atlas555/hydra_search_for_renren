package com.renren.hydra.util;

import java.lang.reflect.Constructor;

import org.apache.log4j.Logger;

public class ReflectUtil {

	private static final Logger logger = Logger.getLogger(ReflectUtil.class);

	public static Object createInstance(String className,
			Class[] constrTypeList, Object[] constrArgList) {
		Class clazz;
		try {
			clazz = Class.forName(className);
			Constructor ctor = clazz.getConstructor(constrTypeList);

			return ctor.newInstance(constrArgList);

		} catch (Exception e) {
			logger.error("ReflectUtil createInstance failed.\nclassName:\t"
					+ className + "\nconstrTypeList:\t" + constrTypeList
					+ "\nconstrArgList:\t" + constrArgList, e);
		}
		return null;

	}

}
