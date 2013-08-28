package com.renren.hydra.attribute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.renren.hydra.config.schema.Schema;

public class DefaultOnlineAttributeData extends OnlineAttributeData {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger
			.getLogger(DefaultOnlineAttributeData.class);
	protected Comparable[] attributeValues;

	public DefaultOnlineAttributeData(Schema schema) {
		super();
		int numOnlineAttributeData = schema.getNumOnlineAttributeDataMeta();
		attributeValues = new Comparable[numOnlineAttributeData];
		for (int i = 0; i < numOnlineAttributeData; ++i)
			attributeValues[i] = null;
	}

	@Override
	public void putAttribute(String key, Comparable value) {
		int index = Schema.getInstance().getOnlineAttributeIdByName(key);
		if (index != -1)
			attributeValues[index] = value;
		else
			logger.error("attribute name not exists in OnlineAttributeMeta: "
					+ key);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		writeAttribute(out);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		readAttribute(in);
	}

	private void writeAttribute(DataOutput out) {
		AttributeDataMeta[] onlineAttributeDatMetas = Schema.getInstance()
				.getOnlineAttributeDataMetas();
		int numOnlineAttributeData = onlineAttributeDatMetas.length;
		for (int i = 0; i < numOnlineAttributeData; ++i) {
			AttributeDataMeta attriDataMeta = onlineAttributeDatMetas[i];
			String name = attriDataMeta.getName();
			AttributeDataMeta.DataType type = attriDataMeta.getType();
			try {
				AttributeSerializeProcessor<Comparable> attributeProcessor = AttributeSerializeProcessor
						.getProcessorInstance(type);
				attributeProcessor.writeAttribute(out, attributeValues[i]);
			} catch (Exception e) {
				logger.error("write attribute :" + name + "\terror:"+e);
			}
		}
	}

	private void readAttribute(DataInput in) {
		AttributeDataMeta[] onlineAttributeDatMetas = Schema.getInstance()
				.getOnlineAttributeDataMetas();
		if (onlineAttributeDatMetas == null
				|| onlineAttributeDatMetas.length == 0)
			return;
		int size = onlineAttributeDatMetas.length;
		for (int i = 0; i < size; i++) {
			String name = onlineAttributeDatMetas[i].getName();
			AttributeDataMeta.DataType type = onlineAttributeDatMetas[i]
					.getType();
			try {
				AttributeSerializeProcessor<Comparable> attributeProcessor = AttributeSerializeProcessor
						.getProcessorInstance(type);
				attributeValues[i] = attributeProcessor.readAttribute(in);
			} catch (Exception e) {
				logger.error("read attribute  :" + name + "\t error:" + e);
			}
		}
	}

	@Override
	public Comparable getAttributeValue(String key) {
		int index = Schema.getInstance().getOnlineAttributeIdByName(key);
		if (index != -1)
			return attributeValues[index];
		else
			return null;
	}

	@Override
	public Comparable getAttributeValue(int index) {
		if (index != -1)
			return attributeValues[index];
		else
			return null;
	}

	@Override
	public Map<String, Comparable> getAttributeDataMap() {
		AttributeDataMeta[] attributeDataMetas = Schema.getInstance()
				.getOnlineAttributeDataMetas();
		int num = attributeDataMetas.length;
		if (num != attributeValues.length)
			return null;
		if (num != 0) {
			Map<String, Comparable> attributeDataMap = new HashMap<String, Comparable>(
					num*2);
			for (int i = 0; i < num; ++i) {
				AttributeDataMeta meta = attributeDataMetas[i];
				String name = meta.getName();
				attributeDataMap.put(name, this.attributeValues[i]);
			}
			return attributeDataMap;
		}
		return null;
	}
}
