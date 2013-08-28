package com.renren.hadoop.index.module;

import org.json.JSONObject;

import proj.zoie.api.indexing.ZoieIndexable;

import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DefaultJsonSchemaInterpreter;

public class JsonSchemaInterpreterForBuildJob extends
		DefaultJsonSchemaInterpreter {
	
	public JsonSchemaInterpreterForBuildJob() {
		super();
	}

	@Override
	public ZoieIndexable convertAndInterpret(final JSONObject obj) {
		return new ZoieIndexableForBuildJob(_schema, obj, _documentProcessor);
	}
}

