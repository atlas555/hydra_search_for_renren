package com.renren.hydra.attribute;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import com.renren.hydra.util.CloseableThreadLocal;

/*
 * 使用ThreadLocal 处理SimpleDateFormat 线程不安全的问题
 * 每一个线程只有一个AttributeDataProcessor 实例
 */

public class AttributeDataProcessorHolder {
	  public static class AttributeDataProcessor {
			private Map<String,SimpleDateFormat> dateFormaters;
			
			private AttributeDataProcessor(){
				this.dateFormaters = new HashMap<String,SimpleDateFormat>();
			}
			
			public Comparable getValue(AttributeDataMeta.DataType type,String extra,String value) throws Exception{
				Comparable ret=null;
				switch (type) {
					case INT:
						ret=Integer.valueOf(value);
						break;
				case LONG:
						ret=Long.valueOf(value);
						break;
				case FLOAT:
						ret=Float.valueOf(value);
						break;
				case DOUBLE:
						ret=Double.valueOf(value);
						break;
				case STRING:
						ret=value;
						break;
				case DATE:
						SimpleDateFormat dateFormater = this.dateFormaters.get(extra);
						if(dateFormater==null){
							dateFormater = new SimpleDateFormat(extra);
							this.dateFormaters.put(extra, dateFormater);
						}
						ret=dateFormater.parse(value);
						break;
				default:
					throw new Exception("can not support data type:" + type);
				}
				return ret;
			}
			
			public  Comparable getValue(AttributeDataMeta attrDataMeta,String value) throws Exception{
				return this.getValue(attrDataMeta.getType(),attrDataMeta.getExtra(),value);
			}
		}
	  
	  private static CloseableThreadLocal<AttributeDataProcessor> processorRef = new CloseableThreadLocal<AttributeDataProcessor>() {
		    protected synchronized AttributeDataProcessor initialValue() {
		      return new AttributeDataProcessor();
		    }
	  };

	  public static AttributeDataProcessor getDataProcessor() {
		    return processorRef.get();
	  }
}
