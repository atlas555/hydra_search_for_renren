package com.renren.hydra.config.schema;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.renren.hydra.attribute.AttributeDataMeta;
import com.renren.hydra.attribute.AttributeDataProcessorHolder;
import com.renren.hydra.search.scorer.ScoreField;
import com.renren.hydra.search.sort.SortField;
import com.renren.hydra.util.IndexFlowFactory;
import com.renren.hydra.util.SchemaUtil;
import com.renren.hydra.util.SearchFlowFactory;

public class Schema {
	private static final Logger logger = Logger.getLogger(Schema.class);

	private static final String FIELD_SCORE_TAG_NAME = "name";
	private static final String FIELD_SCORE_WEIGHT = "weight";
	private static final String FILTER_NODE_TAG = "filter";
	private static final String INDEX_FIELDS_TAG = "index_field";
	private static final String SCORE_FIELDS_TAG = "score_field";
	private static final String SUMMARY_FIELDS_TAG = "summary_field";
	private static final String USER_ID_FIELDS_TAG = "userid_field";
	private static final String USER_ID_FIELD_ATTRIBUTE = "name";
	private static final String FIELD_TAG = "field";
	private static final String ATTRIBUTE_FIELDS_TAG = "attribute_field";
	private static final String ATTRIBUTE_TAG = "attribute";

	protected Map<String, String> beanId2class;
	protected Map<String, String> flowNodeMap;
	protected Map<String, String> indexFieldAnalyzer;
	protected Map<String, String> searchFieldAnalyzer;

	// index
	private String uidField;
	
	private Map<String, FieldDefinition> indexFieldDefMap;
	private Map<Integer, String> indexId2FieldName;
	private Map<String, Integer> indexFieldName2Id;
	private Set<String> indexFields;

	// search
	private Set<String> noHighlightSummaryFields;
	private Set<String> highlightSummaryFields;

	private Map<String, Integer> scoreFieldName2Id;
	private String[] scoreFieldId2Name;

	private int numScoreField;
	private int numIndexField;

	private ScoreField[] scoreFields;
	private String[] scoreFilterNames;

	public Map<String, SortField> attributeSortFields;

	private AttributeDataMeta[] onlineAttributeDataMetas;
	private AttributeDataMeta[] offlineAttributeDataMetas;
	
	private int numOnlineAttributeDataMeta;
	private int numOfflineAttributeDataMeta;
	
	private Map<String,Integer> onlineAttributeName2Id;
	private Map<String,Integer> offlineAttributeName2Id;
	
	private boolean _init;

	private String userIdFieldName;	

    private static class SchemaSingletonHolder{ 
    	public static final Schema INSTANCE = new Schema();
    }

     public static Schema getInstance(){
        return SchemaSingletonHolder.INSTANCE;
     }


	public static class FieldDefinition implements Serializable {
		private static final long serialVersionUID = 1L;

		public Store store;
		public Index index;
		public TermVector tv;
		public MetaType fieldType;
		public float boost;
		public boolean multiValue;

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("store:");
			sb.append(store.toString());
			sb.append("\tindex:");
			sb.append(index.toString());
			sb.append("\tMetaType:");
			sb.append(fieldType.toString());
			sb.append("\tboost:");
			sb.append(boost);
			sb.append("\tTermVector:");
			sb.append(tv.toString());
			sb.append("\tMultivalue:");
			sb.append(multiValue);
			return sb.toString();
		}
	}

	public String getUidField() {
		return uidField;
	}
	
	public String getUserIdFieldName() {
		return userIdFieldName;
	}

	public FieldDefinition getFieldDef(String field) {
		return indexFieldDefMap.get(field);
	}

	// 后添加的，输入是域名，输出是域名所对应的ID
	public String getIndexFieldNameById(int id) {
		return indexId2FieldName.get(id);
	}

	public int getIndexIdByFieldName(String field) {
		return indexFieldName2Id.get(field);
	}

	public int getNumIndexField(){
		return this.numIndexField;
	}
	
	public Set<String> getIndexFields() {
		return indexFields;
	}

	public Map<String, FieldDefinition> getIndexfieldDefMap() {
		return indexFieldDefMap;
	}

	public int getNumScoreField() {
		return numScoreField;
	}
	

	public ScoreField[] getScoreFields() {
		return scoreFields;
	}

	public String[] getScoreFilterNames() {
		return scoreFilterNames;
	}

	public String[] getScoreFiledNames() {
		return this.scoreFieldId2Name;
	}

	public String getScoreFieldNameById(int id) {
		if (0 <= id && id < numScoreField)
			return scoreFieldId2Name[id];
		else
			return null;
	}

	// -1 for error
	public int getScoreFieldIdByName(String fieldname) {
		if (scoreFieldName2Id.containsKey(fieldname))
			return scoreFieldName2Id.get(fieldname);
		else
			return -1;
	}

	public Set<String> getNoHighlightSummaryFields() {
		return this.noHighlightSummaryFields;
	}

	public Set<String> getHighlightSummaryFields() {
		return highlightSummaryFields;
	}

	public AttributeDataMeta[] getOnlineAttributeDataMetas() {
		return onlineAttributeDataMetas;
	}

	public AttributeDataMeta[] getOfflineAttributeDataMetas() {
		return offlineAttributeDataMetas;
	}

	public int getNumOnlineAttributeDataMeta(){
		return this.numOnlineAttributeDataMeta;
	}
	
	public int getNumOfflineAttributeDataMeta(){
		return this.numOfflineAttributeDataMeta;
	}
	
	public int getOnlineAttributeIdByName(String attrName){
		if(this.onlineAttributeName2Id.containsKey(attrName))
			return this.onlineAttributeName2Id.get(attrName);
		else
			return -1;
	}
	public int getOfflineAttributeIdByName(String attrName){
		if(this.offlineAttributeName2Id.containsKey(attrName))
			return this.offlineAttributeName2Id.get(attrName);
		else
			return -1;
	}
	
	public Comparable getDefaultOnlineAttributeValue(String attrName){
		int index = this.getOnlineAttributeIdByName(attrName);
		if(index==-1)
			return null;
		return this.onlineAttributeDataMetas[index].getDefaultValue();
	}
	
	public Comparable getDefaultOnlineAttributeValue(int index){
		if(index==-1)
			return null;
		return this.onlineAttributeDataMetas[index].getDefaultValue();
	}
	
	public Comparable getDefaultOfflineAttributeValue(String attrName){
		int index = this.getOfflineAttributeIdByName(attrName);
		if(index==-1)
			return null;
		return this.offlineAttributeDataMetas[index].getDefaultValue();
	}
	
	public Comparable getDefaultOfflineAttributeValue(int index){
		if(index==-1)
			return null;
		return this.offlineAttributeDataMetas[index].getDefaultValue();
	}
	
	public Map<String,Comparable> getDefaultOnlineAttributeDataMap(){
		if(this.onlineAttributeDataMetas.length==0)
			return null;
		Map<String,Comparable> attributeDataMap = new HashMap<String,Comparable>(this.onlineAttributeDataMetas.length*2);
		for(AttributeDataMeta attrDataMeta:this.onlineAttributeDataMetas){
			attributeDataMap.put(attrDataMeta.getName(), attrDataMeta.getDefaultValue());
		}
		return attributeDataMap;
	}
	
	public Map<String,Comparable> getDefaultOfflineAttributeDataMap(){
		if(this.offlineAttributeDataMetas.length==0)
			return null;
		Map<String,Comparable> attributeDataMap = new HashMap<String,Comparable>(this.offlineAttributeDataMetas.length*2);
		for(AttributeDataMeta attrDataMeta:this.offlineAttributeDataMetas){
			attributeDataMap.put(attrDataMeta.getName(), attrDataMeta.getDefaultValue());
		}
		return attributeDataMap;
	}
	
	private Schema() {
		this.flowNodeMap = new HashMap<String, String>();
		indexFieldDefMap = new HashMap<String, FieldDefinition>();
		indexId2FieldName = new HashMap<Integer, String>();
		indexFieldName2Id = new HashMap<String, Integer>();
		indexFields = new HashSet<String>();
		noHighlightSummaryFields = new HashSet<String>();
		highlightSummaryFields = new HashSet<String>();
		attributeSortFields = new HashMap<String, SortField>();

		this.offlineAttributeDataMetas = new AttributeDataMeta[0];
		this.onlineAttributeDataMetas = new AttributeDataMeta[0];
		scoreFilterNames = new String[0];
	
		this.onlineAttributeName2Id = new HashMap<String,Integer>();
		this.offlineAttributeName2Id = new HashMap<String,Integer>();
		this.numIndexField = 0;
		this.numScoreField = 0;
		this.numOnlineAttributeDataMeta = 0;
		this.numOfflineAttributeDataMeta = 0;
		
		this._init=false;
		this.userIdFieldName = null;
	}

	public String getFlowNodeClass(String module) {
		return flowNodeMap.get(module);
	}

	public String getClassName(String id) {
		return this.beanId2class.get(id);
	}

	public Map<String, String> getIndexFieldAnalyzer() {
		return this.indexFieldAnalyzer;
	}

	public Map<String, String> getSearchFieldAnalyzer() {
		return this.searchFieldAnalyzer;
	}

	public String[] getFilters(Element e) {
		NodeList filterNodes = e.getElementsByTagName(FILTER_NODE_TAG);
		int num = filterNodes.getLength();
		logger.info("num filter: " + num);
		if (0 == num)
			return new String[0];

		List<String> filterNames = new ArrayList<String>();

		for (int i = 0; i < num; ++i) {
			Node filterNode = filterNodes.item(i);
			if (filterNode.getNodeType() == Node.ELEMENT_NODE
					&& filterNode.getNodeName().equals(FILTER_NODE_TAG)) {
				String filterName = ((Element) filterNode).getAttribute("id");
				logger.info("filter type : " + filterName);
				filterNames.add(filterName);
			}

		}
		return filterNames.toArray(new String[filterNames.size()]);
	}
	
	public synchronized boolean initSchema(String schemaFileName) {
		if(this._init){
			logger.info("already init");
			return true;
		}
		logger.info("begin to load schmea file: " + schemaFileName);
		File schemaFile = new File(schemaFileName);
		if (!schemaFile.exists()) {
			logger.error("cannot find file " + schemaFileName);
			return false;
		}
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setIgnoringComments(true);
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document schemaXml = db.parse(schemaFile);
			schemaXml.getDocumentElement().normalize();
			initField(schemaXml);
			parserBeansAndFlow(schemaXml);
			logger.info(this.toString());
			this._init=true;
			return validCheck();
		} catch (Exception e) {
			logger.error(e);
			return false;
		}
	}
	
	public boolean validCheck(){
		 for(ScoreField scoreField : this.scoreFields){
			 if(!this.indexFieldDefMap.containsKey(scoreField.getFieldName())){
				 logger.error("score field must be exists in index field, field:"+scoreField.getFieldName());
				 return false;
			 }
		 }
		return true;
	}

	public boolean initSchema(String confDir, String schemaFileName) {
		String absPath=confDir+File.separator+schemaFileName;
		return initSchema(absPath);
	}

	public void addClassMap(String name, String beanId) throws Exception {
		String className = beanId2class.get(beanId);
		if (className == null || className.equals(""))
			throw new Exception("doesn't has class for id: " + beanId);
		flowNodeMap.put(name, className);
	}

	public void parserFlow(Element flow) throws Exception {
		NodeList modules = flow.getElementsByTagName(SchemaUtil.MODULE_TAG);
		for (int i = 0; i < modules.getLength(); ++i) {
			Node node = modules.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				processFlowModule((Element) node);
			}
		}
	}

	public void processFlowModule(Element module) throws Exception {
		String name = module.getAttribute("name").trim();
		if (name == null)
			throw new Exception(
					"configuration error, module name can not be null");

		if (name.equals(SearchFlowFactory.FilterChain)) {
			scoreFilterNames = getFilters(module);
			int numFilter = scoreFilterNames.length;
			for (int j = 0; j < numFilter; ++j) {
				addClassMap(scoreFilterNames[j], scoreFilterNames[j]);
			}
		} else {
			String id = module.getAttribute("id").trim();
			if (id == null || id.equals(""))
				throw new Exception(
						"module id cann't be null, configuration error for module : "
								+ name);
			addClassMap(name, id);
			if (name.equals(SearchFlowFactory.SearchAnalyzer)) {
				this.searchFieldAnalyzer = SchemaUtil
						.parserFieldAnalyzer(module);
			} else if (name.equals(IndexFlowFactory.IndexAnalyzer)) {
				this.indexFieldAnalyzer = SchemaUtil
						.parserFieldAnalyzer(module);
			}
		}
	}

	public Element getSingleElement(Document doc, String tag) {
		NodeList node = doc.getElementsByTagName(tag);
		Element e = (Element) node.item(0);
		return e;
	}

	public void checkIndexFlowNodes() throws Exception {
		if (!SchemaUtil.checkFlowNode(IndexFlowFactory.VersionComparator,
				flowNodeMap)) {
			flowNodeMap.put(IndexFlowFactory.VersionComparator,
					IndexFlowFactory.DefaultVersionComparatorClass);
		}
		if (!SchemaUtil.checkFlowNode(IndexFlowFactory.DataProvider,
				flowNodeMap)) {
			flowNodeMap.put(IndexFlowFactory.DataProvider,
					IndexFlowFactory.DefaultDataProviderClass);
		}
		if (!SchemaUtil.checkFlowNode(IndexFlowFactory.OnlineAttributeManager,
				flowNodeMap)) {
			flowNodeMap.put(IndexFlowFactory.OnlineAttributeManager,
					IndexFlowFactory.DefaultOnlineAttributeManagerClass);
		}
		if (!SchemaUtil.checkFlowNode(IndexFlowFactory.OfflineAttributeManager,
				flowNodeMap)) {
			flowNodeMap.put(IndexFlowFactory.OfflineAttributeManager,
					IndexFlowFactory.DefaultOfflineAttributeManagerClass);
		}
		if (!SchemaUtil.checkFlowNode(IndexFlowFactory.IndexAnalyzer,
				flowNodeMap)) {
			flowNodeMap.put(IndexFlowFactory.IndexAnalyzer,
					IndexFlowFactory.DefaultIndexAnalyzerClass);
		}
		if (!SchemaUtil.checkFlowNode(IndexFlowFactory.DocumentProcessor,
				flowNodeMap)) {
			flowNodeMap.put(IndexFlowFactory.DocumentProcessor,
					IndexFlowFactory.DefaultDocumentProcessorClass);
		}
		if (!SchemaUtil.checkFlowNode(IndexFlowFactory.JsonSchemaInterpreter,
				flowNodeMap)) {
			flowNodeMap.put(IndexFlowFactory.JsonSchemaInterpreter,
					IndexFlowFactory.DefaultJsonSchemaInterpreterClass);
		}
	}

	public void checkSearchFlowNodes() throws Exception {
		if (!SchemaUtil.checkFlowNode(SearchFlowFactory.QueryParser,
				flowNodeMap)) {
			flowNodeMap.put(SearchFlowFactory.QueryParser,
					SearchFlowFactory.DefaultQueryParserClass);
		}
		if (!SchemaUtil.checkFlowNode(SearchFlowFactory.SearchAnalyzer,
				flowNodeMap)) {
			flowNodeMap.put(SearchFlowFactory.SearchAnalyzer,
					SearchFlowFactory.DefaultSearchAnalyzerClass);
		}
		if (!SchemaUtil.checkFlowNode(SearchFlowFactory.HighlightAnalyzer,
				flowNodeMap)) {
			flowNodeMap.put(SearchFlowFactory.HighlightAnalyzer,
					SearchFlowFactory.DefaultHighlightAnalyzerClass);
		}
		if (!SchemaUtil
				.checkFlowNode(SearchFlowFactory.Similarity, flowNodeMap)) {
			flowNodeMap.put(SearchFlowFactory.Similarity,
					SearchFlowFactory.DefailtSimilarityClass);
		}
		if (!SchemaUtil
				.checkFlowNode(SearchFlowFactory.CoreService, flowNodeMap)) {
			flowNodeMap.put(SearchFlowFactory.CoreService,
					SearchFlowFactory.CoreServiceClass);
		}
		
		if (!SchemaUtil
				.checkFlowNode(SearchFlowFactory.SearchPretreater, flowNodeMap)) {
			flowNodeMap.put(SearchFlowFactory.SearchPretreater,
					SearchFlowFactory.SearchPretreaterClass);
		}
	}

	public void checkFlowNodes() throws Exception {
		checkIndexFlowNodes();
		checkSearchFlowNodes();
	}

	public void parserBeansAndFlow(Document doc) throws Exception {
		this.beanId2class = SchemaUtil.loadBeans(doc);
		Element indexFlow = getSingleElement(doc, SchemaUtil.INDEX_FLOW_TAG);
		parserFlow(indexFlow);
		Element searchFlow = getSingleElement(doc, SchemaUtil.SEARCH_FLOW_TAG);
		parserFlow(searchFlow);
		checkFlowNodes();
	}

	public void initIndexField(Document schemaDoc)
			throws ConfigurationException {
		logger.info("start to build index field.");
		NodeList indexfields = schemaDoc.getElementsByTagName(INDEX_FIELDS_TAG);
		if (indexfields.getLength() < 1) {
			logger.error("can not find indexfields");
			throw new ConfigurationException("can not find index field");
		}
		Element indexfield = (Element) indexfields.item(0);
		uidField = indexfield.getAttribute("uid");
		if (uidField.isEmpty()) {
			throw new ConfigurationException("no uid attribute found");
		}

		logger.info("uid field: " + uidField);
		NodeList fields = indexfield.getElementsByTagName("field");
		if (fields.getLength() < 1) {
			throw new ConfigurationException("no index field find");
		}
		for (int j = 0; j < fields.getLength(); ++j) {
			try {
				Element field = (Element) fields.item(j);
				FieldDefinition fdef = new FieldDefinition();
				String name = field.getAttribute("name");
				if (name.isEmpty()) {
					throw new ConfigurationException(
							"name attribute must be configured");
				}
				// boost值需要从配置文件读入
				fdef.boost = (float) 1.0;
				fdef.fieldType = MetaType.String;
				
				Store storeType = Store.NO;
				String storeTypeStr = field.getAttribute("store");
				if (!storeTypeStr.isEmpty()) {
					if (storeTypeStr.toLowerCase().equals("yes"))
						storeType = Store.YES;
				}
				
				String multiValueStr = field.getAttribute("multivalue");
				boolean multiValue = false;
				if(!multiValueStr.isEmpty()) {
					if(multiValueStr.toLowerCase().equals("true"))
						multiValue = true;
				}
				
				String indexTypeStr = field.getAttribute("analyze");
				Index indexType = Index.ANALYZED_NO_NORMS;
				if (!indexTypeStr.isEmpty()) {
					if (indexTypeStr.toLowerCase().equals("no"))
						indexType = Index.NOT_ANALYZED_NO_NORMS;
				}
				
				if(multiValue) {
					indexType = Index.NOT_ANALYZED_NO_NORMS;
				}

				fdef.index = indexType;
				fdef.store = storeType;
				fdef.tv = TermVector.NO;
				fdef.multiValue = multiValue;

				indexFields.add(name);
				indexId2FieldName.put(j, name);
				indexFieldName2Id.put(name, j);

				indexFieldDefMap.put(name, fdef);
				logger.debug("field name: " + name + ", boost: " + fdef.boost
						+ ", type: " + fdef.fieldType + ", index: "
						+ fdef.index + ", store: " + fdef.store
						+ ", term vector: " + fdef.tv +", multivalue: "+fdef.multiValue);
			} catch (Exception e) {
				throw new ConfigurationException(
						"Error parsing schema in column: " + j
								+ ", error msg: " + e.getMessage());
			}
		}
		this.numIndexField = indexFields.size();
		logger.info("num index fields is :"+this.numIndexField);
	}

	public void initScoreField(Document doc) throws ConfigurationException {
		logger.info("start to build score field.");
		NodeList scoreFieldNodes = doc.getElementsByTagName(SCORE_FIELDS_TAG);
		if (scoreFieldNodes.getLength() < 1) {
			logger.error("can not find scorefields");
			throw new ConfigurationException("can not find score field");
		}
		try {
			NodeList fields = ((Element) scoreFieldNodes.item(0))
					.getElementsByTagName(FIELD_TAG);
			numScoreField = fields.getLength();
			if (numScoreField < 1) {
				logger.error("no any field for scoring");
				throw new ConfigurationException("no any field for scoring");
			}
			scoreFields = new ScoreField[numScoreField];
			scoreFieldName2Id = new HashMap<String, Integer>(numScoreField);
			scoreFieldId2Name = new String[numScoreField];
			for (int i = 0; i < numScoreField; ++i) {
				Element element = (Element) fields.item(i);
				String fieldname = element.getAttribute(FIELD_SCORE_TAG_NAME);
				float weight = Float.parseFloat(element
						.getAttribute(FIELD_SCORE_WEIGHT));
				ScoreField scoreField = new ScoreField(fieldname, weight);
				scoreFields[i] = scoreField;
				scoreFieldId2Name[i] = fieldname;
				scoreFieldName2Id.put(fieldname, i);
			}
		} catch (Exception e) {
			logger.error("parsing score field error:" + e);
			throw new ConfigurationException("parsing score field error");
		}
	}

	public void initSummaryField(Document doc) throws ConfigurationException {
		logger.info("start to build summary field.");
		NodeList summaryfields = doc.getElementsByTagName(SUMMARY_FIELDS_TAG);
		if (summaryfields.getLength() < 1) {
			logger.info("no summary field");
			throw new ConfigurationException("can not find summary field");
		}
		try {
			NodeList nodelist = ((Element) summaryfields.item(0))
					.getElementsByTagName(FIELD_TAG);
			int length = nodelist.getLength();
			for (int j = 0; j < length; ++j) {
				Node node = nodelist.item(j);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element field = (Element) node;
					String name = field.getAttribute("name");
					if (name.isEmpty()) {
						throw new ConfigurationException(
								"parser for summmary field : name attribute must be configured");
					}
					String highlight = field.getAttribute("highlight");
					if (highlight.isEmpty() || !highlight.equals("true")) {
						noHighlightSummaryFields.add(name);
						logger.info("add summary field: " + name
								+ ", no highlight.");
					} else {
						highlightSummaryFields.add(name);
						logger.info("add summary field: " + name
								+ ", highlight.");
					}
				}
			}
		} catch (Exception e) {
			logger.error("parser for summary field error : " + e);
			throw new ConfigurationException("parser for summmary field error");
		}
	}

	public void initAttributeField(Document doc) throws Exception {
		logger.info("start to init attribute field.");
		NodeList attributes = doc.getElementsByTagName(ATTRIBUTE_FIELDS_TAG);
		if (attributes.getLength() == 0) {
			return;
		}
		Element element = (Element) attributes.item(0);
		NodeList attributeNodes = element.getChildNodes();
		int num = attributeNodes.getLength();
		logger.info("num attribute: " + num);
		ArrayList<AttributeDataMeta> onlineAttributeList = new ArrayList<AttributeDataMeta>();
		ArrayList<AttributeDataMeta> offlineAttributeList = new ArrayList<AttributeDataMeta>();

		for (int i = 0; i < num; ++i) {
			Node node = attributeNodes.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE
					&& node.getNodeName().equals(ATTRIBUTE_TAG)) {
				Element e = (Element)node;
				String name = e.getAttribute("name");
				String datasource = e.getAttribute("datasource");
				String type = e.getAttribute("type");
				String extra = e.getAttribute("format");
				Comparable defaultValue=null;
				AttributeDataMeta.DataType dType = AttributeDataMeta.DataType.valueOf(type.toUpperCase());
				if(e.hasAttribute("default")){
					String valueStr=e.getAttribute("default");
					defaultValue=AttributeDataProcessorHolder.getDataProcessor().getValue(dType,extra,valueStr);
				}
				logger.info("name: " + name + " datasource:" + datasource
						+ " type:" + type + " extra:" + extra+" defaultValue:"+defaultValue);
				try {
					SortField sortField = SortField.createSortField(name,
							datasource, type);
					AttributeDataMeta attrDataMeta=new AttributeDataMeta(sortField.getField(),
							dType, extra,defaultValue);
					if (sortField != null) {
						if (sortField.getSourceType() == SortField.DataSourceType.ONLINE) {
							onlineAttributeList.add(attrDataMeta);
						} else if (sortField.getSourceType() == SortField.DataSourceType.OFFLINE) {
							offlineAttributeList.add(attrDataMeta);
						}
						attributeSortFields.put(name, sortField);
					}
				} catch (Exception ex) {
					logger.error(ex);
					throw new ConfigurationException("parser for attrbiute field error");
				}
			}
		}
		this.onlineAttributeDataMetas = onlineAttributeList
				.toArray(new AttributeDataMeta[onlineAttributeList.size()]);
		this.offlineAttributeDataMetas = offlineAttributeList
				.toArray(new AttributeDataMeta[offlineAttributeList.size()]);
		this.numOnlineAttributeDataMeta = this.onlineAttributeDataMetas.length;
		for(int i=0;i<onlineAttributeDataMetas.length;++i){
			this.onlineAttributeName2Id.put(this.onlineAttributeDataMetas[i].getName(), i);
		}
		this.numOfflineAttributeDataMeta = this.offlineAttributeDataMetas.length;
		for(int i=0;i<offlineAttributeDataMetas.length;++i){
			this.offlineAttributeName2Id.put(this.offlineAttributeDataMetas[i].getName(), i);
		}
		
		logger.info("attribute sort fields size :" + attributeSortFields.size()
				+ "\t online attribute data size:"
				+ this.onlineAttributeDataMetas.length
				+ "\t offline attribute data size:"
				+ this.offlineAttributeDataMetas.length);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("-----------------schema--------------------\n");
		sb.append("beans\n");
		for (Map.Entry<String, String> entry : this.beanId2class.entrySet())
			sb.append("\tid:" + entry.getKey() + "\tclass:" + entry.getValue()
					+ "\n");
		sb.append("\n");

		sb.append("flows\n");
		for (Map.Entry<String, String> entry : this.flowNodeMap.entrySet())
			sb.append("\tflow:" + entry.getKey() + "\tclass:"
					+ entry.getValue() + "\n");
		sb.append("\n");

		sb.append("index fields\n");
		for (String indexField : this.indexFields) {
			sb.append("\t" + indexField + "\t"
					+ this.indexFieldDefMap.get(indexField).toString() + "\n");
		}
		sb.append("\n");

		sb.append("score field\n");
		for (ScoreField scoreField : this.scoreFields)
			sb.append("\t" + scoreField.toString() + "\n");
		sb.append("\n");

		sb.append("summary field\n");
		sb.append("highlight summary field:\n");
		for (String field : this.highlightSummaryFields)
			sb.append("\t" + field + "\n");
		sb.append("\n");

		sb.append("nohighlight summary field:\n");
		for (String field : this.noHighlightSummaryFields)
			sb.append("\t" + field + "\n");
		sb.append("\n");

		sb.append("\n");

		sb.append("attribute field\n");
		sb.append("online attribute field\n");
		for (AttributeDataMeta attributeMeta : this.onlineAttributeDataMetas)
			sb.append("\t" + attributeMeta.toString() + "\n");
		sb.append("\n");

		sb.append("offline attribute field\n");
		for (AttributeDataMeta attributeMeta : this.offlineAttributeDataMetas)
			sb.append("\t" + attributeMeta.toString() + "\n");
		sb.append("\n");
		sb.append("\n");

		sb.append("-----------------schema--------------------\n");
		return sb.toString();
	}

	public void initUserIdField(Document doc) throws Exception {
		NodeList userIdField = doc.getElementsByTagName(USER_ID_FIELDS_TAG);
		if (userIdField.getLength() > 1) {
			throw new ConfigurationException("userid field should not be more than one");			
                } else if (userIdField.getLength() == 1) {
			Element element = (Element) userIdField.item(0);
			userIdFieldName = element.getAttribute(USER_ID_FIELD_ATTRIBUTE);
			if (userIdFieldName.isEmpty()) {
				logger.info("no userid field configured!");
				userIdFieldName = null;
 			}
		}
	}

	public void initField(Document doc) throws Exception {
		logger.info("init fields");
		initIndexField(doc);
		initScoreField(doc);
		initSummaryField(doc);
		initAttributeField(doc);
		initUserIdField(doc);
	}
}
