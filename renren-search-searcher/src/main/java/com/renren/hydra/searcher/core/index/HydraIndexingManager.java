package com.renren.hydra.searcher.core.index;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import org.apache.lucene.analysis.Analyzer;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieSystem;

import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeManager;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.util.IndexFlowFactory;
import com.renren.hydra.index.DocumentStateParser;


public class HydraIndexingManager {

	private static final Logger logger = Logger
			.getLogger(HydraIndexingManager.class);

	private static final String CONFIG_PREFIX = "hydra.index.manager.default";
	private static final String PROVIDER_TYPE = "type";
	private static final String EVTS_PER_MIN = "eventsPerMin";
	private static final String BATCH_SIZE = "batchSize";
	private static final String SHARDING_STRATEGY = "shardingStrategy";

	private List<StreamDataProvider<JSONObject>> _dataProviderList;

	private final Schema _schema;
	private final Configuration _hydraConfig;
	private Map<Integer, OnlineAttributeManager> _onlineAttrMgrMap;
	private Map<Integer, ZoieSystem> _zoieSystemMap;
	private final Comparator<String> _versionComparator;
	private Analyzer _analyzer;

	public HydraIndexingManager(Schema schema, Configuration hydraConfig,
			Comparator<String> versionComparator, Analyzer analyzer,
			Map<Integer, OnlineAttributeManager> onlineAttrMgrMap) {
		_dataProviderList = null;
		_hydraConfig = hydraConfig;
		_schema = schema;
		_zoieSystemMap = null;
		_versionComparator = versionComparator;
		_onlineAttrMgrMap = onlineAttrMgrMap;
		_analyzer = analyzer;
	}

	public void initialize(Map<Integer, ZoieSystem> zoieSystemMap)
			throws Exception {

		_zoieSystemMap = zoieSystemMap;
		int partSize = _zoieSystemMap.size();
		_dataProviderList = new ArrayList<StreamDataProvider<JSONObject>>(
				partSize);
		Iterator<Integer> it = zoieSystemMap.keySet().iterator();
		while (it.hasNext()) {
			int part = it.next();
			ZoieSystem zoie = zoieSystemMap.get(part);
			String indexVersion = zoie.getVersion();
			OnlineAttributeManager attrManager = _onlineAttrMgrMap.get(part);
			String attrVersion = attrManager.getCurVersion();

			String dataVersion = null;
			logger.info("indexVersion is: " + indexVersion
					+ "; attrVersion is: " + attrVersion);
			if (_versionComparator.compare(indexVersion, attrVersion) > 0) {
				dataVersion = attrVersion;
			} else {
				dataVersion = indexVersion;
			}

			logger.info("data start version is: " + dataVersion);
			StreamDataProvider<JSONObject> dataProvider = buildDataProvider(
					part, dataVersion);
			
			Analyzer analyzer = IndexFlowFactory.createIndexAnalyzer(_schema);
			if (analyzer == null) {
				logger.error("create analyzer failed");
				throw new Exception("create analyzer failed");
			} else{
				logger.info("create analyzer ok");
			}
			
			DataDispatcher dispatcher = new DataDispatcher(zoie, attrManager,
					analyzer);
			dataProvider.setDataConsumer(dispatcher);
			_dataProviderList.add(dataProvider);
		}
	}

	private StreamDataProvider<JSONObject> buildDataProvider(int partId,
			String version) throws ConfigurationException {

		Configuration myConf = _hydraConfig.subset(CONFIG_PREFIX);

		AbstractDataProvider<JSONObject> dataProvider = null;

		try {
			dataProvider = (AbstractDataProvider<JSONObject>) IndexFlowFactory
					.createDataProvider(_schema, myConf, _versionComparator,
							partId, version);
			dataProvider.init();

			long maxEventsPerMin = myConf.getLong(EVTS_PER_MIN, 40000);
			dataProvider.setMaxEventsPerMinute(maxEventsPerMin);
			int batchSize = myConf.getInt(BATCH_SIZE, 1);
			dataProvider.setBatchSize(batchSize);
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new ConfigurationException(e.getMessage(), e);
		}

		return dataProvider;
	}

	public void shutdown() {
		for (StreamDataProvider<JSONObject> dataProvider : _dataProviderList) {
			if (dataProvider == null) {
				continue;
			}
			dataProvider.stop();
		}
	}

	public void start() throws Exception {
		for (StreamDataProvider<JSONObject> dataProvider : _dataProviderList) {
			if (dataProvider == null) {
				throw new Exception("data provider is not started");
			}
			dataProvider.start();
		}
	}

	private class DataDispatcher implements DataConsumer<JSONObject> {
		private ZoieSystem _zoie;
		private OnlineAttributeManager _onlineAttrManager;
		private String _currentVersion;
		private DocumentProcessor _processor;

		public DataDispatcher(ZoieSystem zoie,
				OnlineAttributeManager onlineAttrManager, Analyzer analyzer) {
			_zoie = zoie;
			_onlineAttrManager = onlineAttrManager;

			_processor = (DocumentProcessor) IndexFlowFactory
					.createDocumentProcessor(_schema, analyzer);
		}

		@Override
		public void consume(Collection<DataEvent<JSONObject>> data)
				throws ZoieException {
			logger.debug("consume the dataList with " + data.size() + " datas");

			ArrayList<DataEvent<JSONObject>> dataList = new ArrayList<DataEvent<JSONObject>>();
			for (DataEvent<JSONObject> dataEvt : data) {
				try {
					JSONObject obj = dataEvt.getData();
					if(null==obj)
						continue;
					
					_currentVersion = dataEvt.getVersion();
					
					DocumentStateParser.DocState docFlag;
					OnlineAttributeData attrData = null;
					long key = _processor.getUid(obj);
					logger.debug("process doc with key " + key
							+ " with version " + _currentVersion);	
					docFlag = DocumentStateParser.getDocState(obj, _onlineAttrManager, key);
					
					switch(docFlag) {
						case DELETE:
							logger.debug("delete doc " + obj);
							_onlineAttrManager.delete(key,_currentVersion);
							dataList.add(dataEvt);
							break;
						case UPDATEATTRONLY:
							logger.debug("updateAttributeData only " + obj);
							attrData = _onlineAttrManager.get(key);
							_processor.processAttribute(obj, attrData);
							_onlineAttrManager.update(key, attrData,_currentVersion);
							break;
						case UPDATE:
							logger.debug("update all " + obj);
							attrData = _processor.process(obj);
							_onlineAttrManager.update(key, attrData,_currentVersion);
							dataList.add(dataEvt);//update index_field,also need to update convert_index
							break;
						case ADD:
							logger.debug("add doc " + obj);
							attrData = _processor.process(obj);
							_onlineAttrManager.add(key, attrData, _currentVersion);
							dataList.add(dataEvt);//for convert_index
							break;
						default:
							logger.debug("get state error for JsonObject" + obj);				
							break;
					}
				} catch (Exception e) {
					JSONObject obj = dataEvt.getData();
					if(obj!=null){
						logger.error("consume data fail.jsonObject: "+ obj.toString(), e);
					}
					else{
						logger.error("consume data fail, jsonObject is null", e);
					}
				}
			}
			
			if(dataList.size()>0){
				logger.debug("consume the dataList with " + dataList.size()
					+ " datas");
				_zoie.consume(dataList);
			}
		}

		@Override
		public String getVersion() {
			return _currentVersion;
		}
	}
}
