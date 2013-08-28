package com.renren.hydra.indexing.provider;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * @author tanbokan
 */
public class LinedJsonFileDataProvider extends
		LinedFileDataProvider<JSONObject> {

	/**
	 * @uml.property name="_dataSourceFilter"
	 * @uml.associationEnd
	 */

	public LinedJsonFileDataProvider(Configuration conf,
			Comparator<String> versionComparator, int partId, String version) {
		super(conf, versionComparator, partId, version);
	}

	@Override
	protected JSONObject convertLine(String line) throws IOException {

		try {
			return new JSONObject(line);
		} catch (JSONException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

}

