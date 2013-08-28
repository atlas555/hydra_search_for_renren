package com.renren.hydra.index;

import java.util.Set;
import java.util.Map;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.config.schema.Schema.FieldDefinition;

import org.apache.commons.configuration.Configuration;

import com.renren.hydra.index.DocumentStateParser;

	public class HydraZoieIndexable extends AbstractZoieIndexable {	
		private static final Logger logger = Logger
						.getLogger(HydraZoieIndexable.class);
		protected Schema schema;
		protected JSONObject obj;
		protected Configuration _config;
		protected DocumentProcessor _documentProcessor;
		

		public HydraZoieIndexable(Schema schema, JSONObject obj, DocumentProcessor dp) {
			this.schema = schema;
			this.obj = obj;
			this._documentProcessor = dp;
		}

		@Override
		public IndexingReq[] buildIndexingReqs() {
			logger.debug("build document from jsonobject: " + obj);
			Document luceneDoc = null;
			luceneDoc = buildDocument();
			return new IndexingReq[] { new IndexingReq(luceneDoc) };
		}

		protected Document buildDocument(){
			Map<String, FieldDefinition> indexFieldDefMap = schema.getIndexfieldDefMap();
			Document luceneDoc = new Document();
			
			Set<String> keys = indexFieldDefMap.keySet();
			for (Iterator<String> it = keys.iterator(); it.hasNext();) {
				String fieldName = it.next();
				if(obj.has(fieldName)){
					FieldDefinition fieldDef = indexFieldDefMap.get(fieldName);
					if(fieldDef.multiValue) {
						JSONArray values = obj.optJSONArray(fieldName);
						if(values!=null){
							int cnt = values.length();
							for(int i = 0;i < cnt;i++) {
								Field field = new Field(fieldName,values.optString(i), fieldDef.store, fieldDef.index, fieldDef.tv);
								luceneDoc.add(field);
							}
						}
					} else {			
						String fieldValue = obj.optString(fieldName);
						Field field = new Field(fieldName, fieldValue, 
								fieldDef.store, fieldDef.index, fieldDef.tv);
						luceneDoc.add(field);
					}
				}
			}			
			return luceneDoc;
		}

		/*
		 * 需要修改获取UID的方式
		 */
		@Override
		public long getUID() {					
			return _documentProcessor.getUid(obj);
		}

		@Override
		public boolean isDeleted() {
			return DocumentStateParser.isDeleted(obj);
		}

		@Override
		public boolean isSkip() {
			return DocumentStateParser.isSkip(obj);
		}
	}	
