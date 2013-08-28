package com.renren.hydra.documentProcessor;

import java.math.BigInteger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;

import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.index.DocumentStateParser;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.attribute.ShareOnlineAttributeData;

public class ShareDocumentProcessor extends DocumentProcessor {
	private static final Logger logger = Logger
			.getLogger(DocumentProcessor.class);

	private DateFormat _dateFormat;

	public ShareDocumentProcessor(Analyzer analyzer, Schema schema) {
		super(analyzer, schema);
		_dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	@Override
	public long getUid(JSONObject obj) {
		String hashStr = obj.optString(_schema.getUidField());
		return getHashValue(hashStr);
	}

	public static long getHashValue(String hashStr) {
		String low64 = hashStr.substring(16);
		long hashValue = new BigInteger(low64, 16).longValue();
		return hashValue & 0x7FFFFFFFFFFFFFFFL;
	}

	@Override
	public OnlineAttributeData createAttributeData() {
		ShareOnlineAttributeData attrData = new ShareOnlineAttributeData(false);
		return attrData;
	}

	@Override
	public void processAttribute(JSONObject obj, OnlineAttributeData attrData) {
		long id = obj.optLong("id");
		int userId = obj.optInt("user_id");
		ShareOnlineAttributeData _attrData = (ShareOnlineAttributeData) attrData;
		if (obj.optBoolean(DocumentStateParser.DELETE_FIELD, false)) {
			_attrData.deleteId(id, userId);
		} else {
			String dateStr = obj.optString("creation_date", null);
			Date date;
			try {
				date = _dateFormat.parse(dateStr);
				_attrData.addItem(id, userId, date.getTime());
			} catch (ParseException e) {
				logger.error(e);
			}
		}
	}

	@Override
	public void preProcess(JSONObject obj) {
	}
}
