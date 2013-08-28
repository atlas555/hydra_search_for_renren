package com.renren.hadoop.index.module;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.index.HydraZoieIndexable;



public class ZoieIndexableForBuildJob extends HydraZoieIndexable {	
	private static final Logger logger = Logger.getLogger(
			ZoieIndexableForBuildJob.class);

	public static final String uidTermVal = "_UID";
	public ZoieIndexableForBuildJob(Schema schema, JSONObject obj, DocumentProcessor dp) {
		super(schema, obj, dp);
	}

	@Override
	public IndexingReq[] buildIndexingReqs() {
		logger.debug("build document from jsonobject: " + obj);
		
		Document luceneDoc = buildDocument();
		long uid = getUID();

		StringBuilder sb = new StringBuilder();
		sb.append(uidTermVal).append("|").append(Long.toString(uid));
		logger.debug("add field " + AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD +
				" with value " + sb.toString());
		Field field = new Field(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD,
			sb.toString(), Field.Store.NO, Field.Index.ANALYZED_NO_NORMS,
			Field.TermVector.NO);
		luceneDoc.add(field);

		return new IndexingReq[] { new IndexingReq(luceneDoc) };
	}
}	

