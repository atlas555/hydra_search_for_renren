package com.renren.hydra.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.queryParser.QueryParser;

public class FieldQueryCondition extends Condition {
	private static final long serialVersionUID = 1L;
	
	private Map<String,String> fieldQuery ;
	
	public FieldQueryCondition(){
		super();
		this.fieldQuery = new HashMap<String,String>();
	}

	public void addFieldQuery(String field,String query) {
		this.fieldQuery.put(field, query);
	}

	public Map<String,String> getFieldQuery() {
		return fieldQuery;
	}
	
	public String getQuery(){
		StringBuffer sb = new StringBuffer();
		sb.append(QueryParser.escape(this.getOrginalQuery()));
		
		Iterator<Map.Entry<String,String> > iter= this.fieldQuery.entrySet().iterator();
		
		while (iter.hasNext()) {
			sb.append(" ");
		    Map.Entry<String,String> entry =  iter.next();
		    String fieldName = entry.getKey();
		    String query = entry.getValue();
		    
		    sb.append(fieldName);
		    sb.append(":(");
		    sb.append(QueryParser.escape(query));
		    sb.append(")");
		}
		return sb.toString().trim();
	}
	
	@Override 
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for (Map.Entry<String, String> entry : fieldQuery.entrySet()){
	           String field = entry.getKey();
	           String query = entry.getValue();
	           sb.append("field:");
	           sb.append(field);
	           sb.append("\t");
	           sb.append("query:");
	           sb.append(query);
	           sb.append("\n");
	        }
		sb.append(super.toString());
		return sb.toString();
	}
	
	
}
