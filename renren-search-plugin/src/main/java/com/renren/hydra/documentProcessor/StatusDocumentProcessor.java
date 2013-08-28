package com.renren.hydra.documentProcessor;

import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.json.JSONException;
import org.json.JSONObject;

import com.renren.hydra.attribute.AttributeDataMeta;
import com.renren.hydra.attribute.DefaultOnlineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;

public class StatusDocumentProcessor extends DocumentProcessor {

    private AttributeDataMeta[] attriDataMetaArray;

    private SimpleDateFormat format;

    public StatusDocumentProcessor(Analyzer analyzer, Schema schema) {
            super(analyzer, schema);
            attriDataMetaArray = _schema.getOnlineAttributeDataMetas();
    }

    @Override
    public OnlineAttributeData createAttributeData() {
            DefaultOnlineAttributeData attrData = new DefaultOnlineAttributeData(this._schema);
            return attrData;
    }

    @Override
    public void processAttribute(JSONObject obj, OnlineAttributeData attrData)
                    throws Exception {
            for (AttributeDataMeta attriDataMeta : attriDataMetaArray) {
                    String name = attriDataMeta.getName();
                    AttributeDataMeta.DataType type = attriDataMeta.getType();
                    String value = obj.getString(name);

                    switch (type) {
                    case INT:
                            attrData.putAttribute(name, Integer.valueOf(value));
                            break;
                    case LONG:
                            attrData.putAttribute(name, Long.valueOf(value));
                            break;
                    case FLOAT:
                            attrData.putAttribute(name, Float.valueOf(value));
                            break;
                    case DOUBLE:
                            attrData.putAttribute(name, Double.valueOf(value));
                            break;
                    case STRING:
                            attrData.putAttribute(name, value);
                            break;
                    case DATE:
                            if (format == null) {
                                    format = new SimpleDateFormat(attriDataMeta.getExtra());
                            }
                            attrData.putAttribute(name, format.parse(value));
                            break;
                    default:
                            throw new Exception("can not support data type:" + type);
                    }
            }

    }

    @Override
    public long getUid(JSONObject obj) {
            return obj.optLong(_schema.getUidField());
    }

    @Override
	public void preProcess(JSONObject obj) {
    	
		if (obj.optString("meta").equals("")) {
			try {
				obj.put("isRepeat", 0);
			} catch (JSONException e) {
				logger.error(e);
			}
		} else {
			try {
				obj.put("isRepeat", 1);
			} catch (JSONException e) {
				logger.error(e);
			}
		}
		String content = obj.optString("content");
		content = content.replaceAll("转自", " ");
		content = content.replaceAll("@", " ");
		if (content.contains("http://rrurl.cn")) {
			int begin = content.indexOf("http://rrurl.cn");
			int end = begin + 16;
			int length = content.length();
			while (end < length
					&& (Character.isLetter(content.charAt(end)) || Character
							.isDigit(content.charAt(end)))) {
				end++;
			}
			if (end >= length)
				end = length;
			Pattern pattern1 = Pattern.compile(content.substring(begin, end));
			content = pattern1.matcher(content).replaceAll(" ");
		}
		String filterStr = "\\((谄笑|吃饭|调皮|尴尬|汗|惊恐|囧|可爱|酷|流口水|生病|叹气|淘气|舔"
				+ "|偷笑|吻|晕|住嘴|大笑|害羞|口罩|哭|困|难过|生气|书呆子|微笑|不|惊讶|kb|sx"
				+ "|ju|gl|yl|hold1|cold|nuomi|feng|hot|rs|sbq|th|mb|tucao|禅师|冰棒"
				+ "|see|zy|by|bs|good|ga)\\)|\\([0-9]*\\)";
		Pattern pattern = Pattern.compile(filterStr);
		Matcher matcher = pattern.matcher(content);
		StringBuffer sb = new StringBuffer();
		boolean result = matcher.find();
		while (result) {
			matcher.appendReplacement(sb, "");
			result = matcher.find();
		}
		matcher.appendTail(sb);
		try {
			obj.put("content", sb.toString().trim());
		} catch (JSONException e) {
			logger.error(e);
		}

	}

}
