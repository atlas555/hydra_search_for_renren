package com.renren.hydra.config;

import java.io.File;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.renren.hydra.config.schema.Schema;

/*
 * HydraConfig 负责读入配置文件，初始化IndexSchema, ApplicationContent, SearchSchema, Log 
 */
public class HydraConfig {

	private static class HydraConfigHolder {
		public static final HydraConfig INSTANCE = new HydraConfig();
	}

	public static HydraConfig getInstance() {
		return HydraConfigHolder.INSTANCE;
	}

	private HydraConfig() {
	}

	private static final Logger logger = Logger.getLogger(HydraConfig.class);

	public static final String HYDRA_PROPERTIES = "hydra.properties";
	public static final String ANALYZER_FILE_XML = "analyzer.xml";
	public static final String PLUGINS = "plugins.xml";
	public static final String LOG_CONF_FILE = "log4j.properties";
	public static final String MMSEG_DICT_DATA_DIR = "mmseg";
	public static final String ARES_ANALYZER_DIR = "ares";
	public static final String SCHEMA_FILE_XML = "schema.xml";
	public static final String TC_DICT_DATA_DIR = "tc";

	private PropertiesConfiguration _hydraConf;
	private Configuration _zkProperties;
	private Schema _schema;

	public boolean init(String rootConfPath) {
		_hydraConf = null;
		logger.info("rootConfPath:" + rootConfPath);
		loadLogConfFile(rootConfPath);
		System.setProperty("mmseg.dic.path", rootConfPath + "/data/"
				+ MMSEG_DICT_DATA_DIR);
		System.setProperty("ares.analyzer.path", rootConfPath + "/data/"
				+ ARES_ANALYZER_DIR);
		System.setProperty("tc.analyzer.path", rootConfPath + "/data/"
				+ TC_DICT_DATA_DIR);
		if (!loadHydraProperties(rootConfPath)) {
			logger.error("init configuration fail! fail to load hydra properties file ");
			return false;
		}

		if (!loadZkProperties(rootConfPath)) {
			logger.error("init configuration fail! fail to load zookeeper conf file");
			return false;
		}

		if (!loadSchema(rootConfPath)) {
			logger.error("init configuration fail! fail to load search schema file");
			return false;
		}

		return true;
	}

	private boolean loadSchema(String confDir) {
		_schema = Schema.getInstance();
		return _schema.initSchema(confDir, SCHEMA_FILE_XML);
	}

	private boolean loadHydraProperties(String confDir) {
		logger.info("begin to load hydra config file " + HYDRA_PROPERTIES);
		File confFile = new File(confDir, HYDRA_PROPERTIES);
		if (!confFile.exists()) {
			logger.error("cannot find " + HYDRA_PROPERTIES + " in dir "
					+ confDir);
			return false;
		}

		_hydraConf = new PropertiesConfiguration();
		_hydraConf.setDelimiterParsingDisabled(true);

		try {
			_hydraConf.load(confFile);
		} catch (ConfigurationException e) {
			logger.error(e.getMessage());
			return false;

		}
		String business = _hydraConf.getString("business",null);
		if(business==null||business.isEmpty()){
			logger.error("please specific business in hydra.properties");
			return false;
		}

		return true;
	}

	private boolean loadZkProperties(String confDir) {
		logger.info("begin to load hydra config file " + HYDRA_PROPERTIES);
		File confFile = new File(confDir, HYDRA_PROPERTIES);
		if (!confFile.exists()) {
			logger.error("cannot find " + HYDRA_PROPERTIES + " in dir "
					+ confDir);
			return false;
		}

		PropertiesConfiguration tmp = new PropertiesConfiguration();
		tmp.setDelimiterParsingDisabled(true);

		try {
			tmp.load(confFile);
			_zkProperties = tmp.subset("zookeeper");
		} catch (ConfigurationException e) {
			logger.error(e.getMessage());
			return false;
		}

		return true;
	}

	private void loadLogConfFile(String confDir) {
		PropertyConfigurator.configure(confDir + "/" + LOG_CONF_FILE);
	}

	public Configuration getHydraConfig() {
		return _hydraConf;
	}

	public Configuration getZkProperties() {
		return _zkProperties;
	}

	public Schema getSchema() {
		return _schema;
	}
}
