package com.renren.hydra.index;

import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;


import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.util.IndexFlowFactory;


public class DefaultJsonSchemaInterpreter extends
		AbstractZoieIndexableInterpreter<JSONObject> {
	
	protected final Schema _schema;
	protected DocumentProcessor _documentProcessor;
	
	public DefaultJsonSchemaInterpreter() {
		_schema = Schema.getInstance();
		_documentProcessor = (DocumentProcessor)IndexFlowFactory.createDocumentProcessor(_schema,null);
	}

	@Override
	public ZoieIndexable convertAndInterpret(final JSONObject obj) {
		return new HydraZoieIndexable(_schema, obj, _documentProcessor);
	}
}
