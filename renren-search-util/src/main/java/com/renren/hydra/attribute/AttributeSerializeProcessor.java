package com.renren.hydra.attribute;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Date;

/*
 * 负责AttributeData的序列化和反序列化 (SequenceFile)
 */
public abstract class AttributeSerializeProcessor<T extends Comparable> {
	public abstract void writeAttribute(DataOutput out, T value)
			throws Exception;

	public abstract T readAttribute(DataInput in) throws Exception;

	public static class DoubleAttributeSerializeProcessor extends
			AttributeSerializeProcessor<Double> {

		private final static DoubleAttributeSerializeProcessor instance = new DoubleAttributeSerializeProcessor();

		private DoubleAttributeSerializeProcessor() {

		}

		public static DoubleAttributeSerializeProcessor getInstance() {
			return instance;
		}

		@Override
		public Double readAttribute(DataInput in) throws Exception {
			return in.readDouble();
		}

		@Override
		public void writeAttribute(DataOutput out, Double value)
				throws Exception {
			out.writeDouble(value);
		}
	}

	public static class FloatAttributeSerializeProcessor extends
			AttributeSerializeProcessor<Float> {

		private final static FloatAttributeSerializeProcessor instance = new FloatAttributeSerializeProcessor();

		private FloatAttributeSerializeProcessor() {

		}

		public static FloatAttributeSerializeProcessor getInstance() {
			return instance;
		}

		@Override
		public Float readAttribute(DataInput in) throws Exception {
			return in.readFloat();
		}

		@Override
		public void writeAttribute(DataOutput out, Float value)
				throws Exception {
			out.writeFloat(value);

		}
	}

	public static class IntegerAttributeSerializeProcessor extends
			AttributeSerializeProcessor<Integer> {

		private final static IntegerAttributeSerializeProcessor instance = new IntegerAttributeSerializeProcessor();

		private IntegerAttributeSerializeProcessor() {

		}

		public static IntegerAttributeSerializeProcessor getInstance() {
			return instance;
		}

		@Override
		public Integer readAttribute(DataInput in) throws Exception {
			return in.readInt();
		}

		@Override
		public void writeAttribute(DataOutput out, Integer value)
				throws Exception {
			out.writeInt(value);

		}
	}

	public static class LongAttributeSerializeProcessor extends AttributeSerializeProcessor<Long> {

		private final static LongAttributeSerializeProcessor instance = new LongAttributeSerializeProcessor();

		private LongAttributeSerializeProcessor() {

		}

		public static LongAttributeSerializeProcessor getInstance() {
			return instance;
		}

		@Override
		public Long readAttribute(DataInput in) throws Exception {
			return in.readLong();
		}

		@Override
		public void writeAttribute(DataOutput out, Long value) throws Exception {
			out.writeLong(value);
		}

	}

	public static class StringAttributeSerializeProcessor extends
			AttributeSerializeProcessor<String> {

		private final static StringAttributeSerializeProcessor instance = new StringAttributeSerializeProcessor();

		private StringAttributeSerializeProcessor() {

		}

		public static StringAttributeSerializeProcessor getInstance() {
			return instance;
		}

		@Override
		public String readAttribute(DataInput in) throws Exception {
			return in.readUTF();
		}

		@Override
		public void writeAttribute(DataOutput out, String value)
				throws Exception {
			out.writeUTF(value);
		}

	}

	public static class DateAttributeSerializeProcessor extends AttributeSerializeProcessor<Date> {

		private final static DateAttributeSerializeProcessor instance = new DateAttributeSerializeProcessor();

		private DateAttributeSerializeProcessor() {

		}

		public static DateAttributeSerializeProcessor getInstance() {
			return instance;
		}

		@Override
		public Date readAttribute(DataInput in) throws Exception {
			return new Date(in.readLong());
		}

		@Override
		public void writeAttribute(DataOutput out, Date value) throws Exception {
			out.writeLong(value.getTime());
		}

	}

	public static AttributeSerializeProcessor getProcessorInstance(
			AttributeDataMeta.DataType type) throws Exception {
		switch (type) {
		case INT:
			return IntegerAttributeSerializeProcessor.instance;
		case FLOAT:
			return FloatAttributeSerializeProcessor.instance;
		case LONG:
			return LongAttributeSerializeProcessor.instance;
		case DOUBLE:
			return DoubleAttributeSerializeProcessor.instance;
		case STRING:
			return StringAttributeSerializeProcessor.instance;
		case DATE:
			return DateAttributeSerializeProcessor.instance;
		default:
			throw new Exception("can not support type:" + type.toString());
		}

	}
	
	public static AttributeSerializeProcessor getProcessorInstance(
			Comparable value) throws Exception {
		if(value instanceof java.lang.Integer)
			return IntegerAttributeSerializeProcessor.instance;
		else if(value instanceof java.lang.Float)
			return FloatAttributeSerializeProcessor.instance;
		else if(value instanceof java.lang.Long)
			return LongAttributeSerializeProcessor.instance;
		else if(value instanceof java.lang.Double)
			return DoubleAttributeSerializeProcessor.instance;
		else if(value instanceof java.lang.String)
			return StringAttributeSerializeProcessor.instance;
		else if(value instanceof java.util.Date)
			return DateAttributeSerializeProcessor.instance;
		else
			throw new Exception("can not support type:" + value.getClass());
	}
}
