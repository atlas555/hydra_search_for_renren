package com.renren.hydra.index;

import java.io.StringReader;
import java.util.Map;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Field.Index;

import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.config.schema.Schema.FieldDefinition;

public abstract class DocumentProcessor {
	protected static Logger logger = Logger.getLogger(DocumentProcessor.class);

	protected Analyzer _analyzer;
	protected Schema _schema;

	public DocumentProcessor(Analyzer analyzer, Schema schema) {
		_analyzer = analyzer;
		_schema = schema;
	}

	public OnlineAttributeData process(JSONObject obj) throws Exception {
		logger.debug("process obj :" + obj);
		preProcess(obj);
		OnlineAttributeData attrData = createAttributeData();
		processTTF(obj, attrData);
		processAttribute(obj, attrData);
		processUserId(obj, attrData);
		return attrData;
	}

	public void processUserId(JSONObject obj, OnlineAttributeData attrData) {
		String fileName = _schema.getUserIdFieldName();
		if (fileName != null) {
			int userId = obj.optInt(fileName, 0);
			attrData.setUserId(userId);
		}
	}

	public void processTTF(JSONObject obj, OnlineAttributeData attrData)
			throws Exception {
		int numFields = _schema.getIndexFields().size();
		for (int i = 0; i < numFields; i++) {
			String fieldName = _schema.getIndexFieldNameById(i);
			int ttf;
			ttf = analyze(obj, fieldName);
			attrData.setTTF(fieldName, ttf);
		}
	}

	//multiValue一定是NOT_ANALYZED_NO_NORMS
	protected int analyze(JSONObject obj, String fieldName) throws Exception {
		if(!obj.has(fieldName))
			return 0;
		Map<String, FieldDefinition> indexFieldDefMap = _schema.getIndexfieldDefMap();		
		FieldDefinition fieldDef = indexFieldDefMap.get(fieldName);
		boolean multiValue = fieldDef.multiValue;		
		String fieldValue = obj.optString(fieldName);
		Index indexType = fieldDef.index;
		
		if(multiValue){
			JSONArray arr = obj.optJSONArray(fieldName);
			if(arr==null)
				return 0;
			else
				return arr.length();
		}else{
			if(indexType==Index.NOT_ANALYZED_NO_NORMS)
				return 1;
		}
		
		TokenStream ts = _analyzer.tokenStream(fieldName, new StringReader(
				fieldValue));
		TermAttribute termAtt = ts.getAttribute(TermAttribute.class);
		StringBuilder sb = new StringBuilder();
		int termCount = 0;
		while (ts.incrementToken()) {
			String term = termAtt.term();
			sb.append(term);
			sb.append(" ");
			termCount++;
		}

		obj.put(fieldName, sb.toString().trim());
		return termCount;
	}

	public abstract void preProcess(JSONObject obj);
	public abstract long getUid(JSONObject obj);
	public abstract OnlineAttributeData createAttributeData();
	public abstract void processAttribute(JSONObject obj, OnlineAttributeData attrData) throws Exception;
}
