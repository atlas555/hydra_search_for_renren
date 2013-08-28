package com.renren.hydra.attribute;

import java.io.Serializable;

public class AttributeDataMeta implements Serializable{
	public static enum DataType {
		INT, FLOAT, LONG, DOUBLE, STRING, DATE;
	}

	private String name;
	private DataType type;

	private String extra;
	private Comparable defaultValue;

	public AttributeDataMeta() {
		this(null, null, null,null);
	}

	public AttributeDataMeta(String name, DataType type, String extra,Comparable defaultValue) {
		this.name = name;
		this.type = type;
		this.extra = extra;
		this.defaultValue=defaultValue;
	}

	public boolean isValid() {
		if (this.name == null || this.name.equals("") || this.type == null)
			return false;
		if(this.type==DataType.DATE && (this.extra==null||this.extra.equals("")))
			return false;
		return true;
	}

	public String getName() {
		return name;
	}

	public DataType getType() {
		return type;
	}

	public String getExtra() {
		return extra;
	}
	
	public Comparable getDefaultValue(){
		return this.defaultValue;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("name:");
		sb.append(this.name);
		sb.append("\ttype:");
		sb.append(this.type.toString());

		if (this.extra != null) {
			sb.append("\textra:");
			sb.append(this.extra);
		}

		if (this.defaultValue != null) {
			sb.append("\tdefaultValue:");
			sb.append(this.defaultValue);
		}
		return sb.toString();
	}

}
