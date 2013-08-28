package com.renren.hydra.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Document;

/*
 * 读取Schema文件
 */
public class SchemaUtil {

	/*
	 * 获取Schmea文件中的beans,返回id到name的映射
	 */
	private static Logger logger = Logger.getLogger(SchemaUtil.class);

	public static final String BEANS_TAG = "beans";
	public static final String BEAN_TAG = "bean";
	public static final String FLOW_TAG = "flow";
	public static final String INDEX_FLOW_TAG = "index_flow";
	public static final String SEARCH_FLOW_TAG = "search_flow";
	public static final String MODULE_TAG = "module";
	public static final String ANALYZER_TAG = "analyzer";

	public static Map<String, String> loadBeans(Document doc) throws Exception {
		logger.info("load beans");
		Map<String, String> id2class = new HashMap<String, String>();
		NodeList beansNodes = doc.getElementsByTagName(BEANS_TAG);

		int num = beansNodes.getLength();

		logger.info("num of tag beans : " + num);
		for (int i = 0; i < num; ++i) {
			Node beansNode = beansNodes.item(i);
			NodeList beans = beansNode.getChildNodes();

			if (null == beans || beans.getLength() == 0)
				continue;

			int beanNum = beans.getLength();
			for (int j = 0; j < beanNum; ++j) {
				Node bean = beans.item(j);
				if (bean.getNodeType() == Node.ELEMENT_NODE
						&& bean.getNodeName().equals(BEAN_TAG)) {
					String beanName = ((Element) bean).getAttribute("id");
					String beanClass = ((Element) bean).getAttribute("class");
					logger.info("bean id : " + beanName + "\t class: "
							+ beanClass);
					id2class.put(beanName, beanClass);
				}
			}
		}
		return id2class;
	}
	
	public static void checkBeans(String[] ids, Map<String,String> id2class) throws Exception{
		int num = ids.length;
		for(int i=0;i<num;++i){
			if(ids[i]==null || (!id2class.containsKey(ids[i])))
				throw new Exception("doesn't has class for id :"+ids[i]);
		}
	}

	public static Map<String, String> loadFlow(Document doc) throws Exception {
		logger.info("load flow");
		Map<String, String> name2Id = new HashMap<String, String>();
		NodeList flowNodes = doc.getElementsByTagName(SchemaUtil.FLOW_TAG);

		if (flowNodes != null && flowNodes.getLength() > 0) {
			Node flowNode = flowNodes.item(0);

			NodeList moduleNodes = flowNode.getChildNodes();
			int moduleNum = moduleNodes.getLength();
			for (int i = 0; i < moduleNum; i++) {
				Node module = moduleNodes.item(i);
				if (module.getNodeType() == Node.ELEMENT_NODE
						&& module.getNodeName().equals(SchemaUtil.MODULE_TAG)) {

					String name = ((Element) module).getAttribute("name");
					String id = ((Element) module).getAttribute("id");
					logger.info("module name : " + name + "\t id: " + id);
					name2Id.put(name, id);
				}

			}

		}

		return name2Id;
	}
	
	public static boolean checkFlowNode(String flowName, Map<String,String> flowMap){
		if(!flowMap.containsKey(flowName))
			return false;
		String classId = flowMap.get(flowName);
		if(classId==null || classId.trim().equals(""))
			return false;
		
		return true;
	}
	
	
	public static Map<String,String> parserFieldAnalyzer(Element element){
		logger.info("parser field analyzer");
		Map<String,String> field2Analyzer = new HashMap<String,String>();
		NodeList nodes = element.getChildNodes();
		if (null == nodes || nodes.getLength() == 0)
			return field2Analyzer;
		
		int num = nodes.getLength();
		for (int i = 0; i < num; ++i) {
			Node node = nodes.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE
					&& node.getNodeName().equals(ANALYZER_TAG)) {
				String field = ((Element) node).getAttribute("field");
				String analyzerId = ((Element) node).getAttribute("id");
				if(null!=field && null!=analyzerId && !field.equals("") && !analyzerId.equals("")){
					field2Analyzer.put(field, analyzerId);
					logger.info("field name: "+field+"\t analyzer type: "+analyzerId);
				}
			}
		}
		return field2Analyzer;
	}
}
