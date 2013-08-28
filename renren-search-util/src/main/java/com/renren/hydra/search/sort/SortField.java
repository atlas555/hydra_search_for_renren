package com.renren.hydra.search.sort;

import java.io.Serializable;

public class SortField implements Serializable {
	private static final long serialVersionUID = 1L;

	public static class SortFieldNotValidException extends Exception {
		private static final long serialVersionUID = 1L;

		public SortFieldNotValidException(String msg) {
			super(msg);
		}

		public SortFieldNotValidException(SortField sortField) {
			super("error sortfiled :" + sortField.toString());
		}

	}

	public static final String SCORE = "score";
	public static final String DOC = "doc";
	public static final String UID = "uid";

	public static enum DataSourceType {
		RESERVE, INDEX, ONLINE, OFFLINE;
	}

	public static enum DataType {
		SCORE, DOC, UID, INT, LONG, FLOAT, DOUBLE, STRING, DATE, CUSTOM;
	}

	private boolean reverse;
	private String field;
	private DataSourceType sourceType;
	private DataType type;

	public boolean isReverse() {
		return reverse;
	}

	public void setReverse(boolean reverse) {
		this.reverse = reverse;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public DataSourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(DataSourceType sourceType) {
		this.sourceType = sourceType;
	}

	public DataType getType() {
		return type;
	}

	public void setType(DataType type) {
		this.type = type;
	}

	public SortField(String field, DataSourceType dsType, DataType dType,
			boolean reverse) {
		this.field = field;
		this.sourceType = dsType;
		this.type = dType;
		this.reverse = reverse;
	}

	public SortField(String field) {
		this(field, false);
	}

	public SortField(String field, boolean reverse) {
		this(field, null, null, reverse);
	}

	public SortField(String field, DataSourceType dsType, DataType dType) {
		this(field, dsType, dType, false);
	}

	public SortField(String field, DataSourceType dsType, DataType dType,
			boolean reverse, FieldComparator comparator) {
		this(field, dsType, dType, false);
	}

	public FieldComparator getComparator() {
		if (null == this.type)
			return null;
		switch (this.type) {
		case INT:
			return new FieldComparator.IntComparator();
		case LONG:
			return new FieldComparator.LongComparator();
		case FLOAT:
			return new FieldComparator.FloatComparator();
		case DOUBLE:
			return new FieldComparator.DoubleComparator();
		case STRING:
			return new FieldComparator.StringComparator();
		case SCORE:
			return new FieldComparator.FloatComparator();
		case DOC:
			return new FieldComparator.IntComparator();
		case UID:
			return new FieldComparator.LongComparator();
		case DATE:
			return new FieldComparator.DateComparator();
		default:
			return null;
		}
	}

	public boolean isTypeValid(Comparable value) {
		if (value == null)
			return false;
		boolean ret = true;
		switch (this.type) {
		case INT:
		case DOC:
			ret = value instanceof java.lang.Integer;
			break;
		case FLOAT:
		case SCORE:
			ret = value instanceof java.lang.Float;
			break;
		case DOUBLE:
			ret = value instanceof java.lang.Double;
			break;
		case STRING:
			ret = value instanceof java.lang.String;
			break;
		case LONG:
		case UID:
			ret = value instanceof java.lang.Long;
			break;
		case DATE:
			ret = value instanceof java.util.Date;
			break;
		default:
			ret = false;
			break;
		}
		return ret;
	}

	public static SortField createSortField(String name, String datasource,
			String type) throws IllegalArgumentException {
		DataSourceType dsType = DataSourceType
				.valueOf(datasource.toUpperCase());
		DataType dType = DataType.valueOf(type.toUpperCase());
		return new SortField(name, dsType, dType);
	}

	public boolean isValid() {
		if (this.sourceType == null || this.field == null || this.type == null)
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("field:");
		sb.append(this.field);
		sb.append("\t");
		sb.append("dataSourceType:");
		sb.append(this.sourceType == null ? "null" : this.sourceType.toString());
		sb.append("\t");
		sb.append("dataType:");
		sb.append(this.type == null ? "null" : type.toString());
		return sb.toString();
	}
}
