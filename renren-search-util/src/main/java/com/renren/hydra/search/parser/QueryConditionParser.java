package com.renren.hydra.search.parser;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import com.renren.hydra.client.Condition;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.search.sort.SortField;
import com.renren.hydra.search.sort.SortField.SortFieldNotValidException;


public abstract class QueryConditionParser {
	protected Analyzer _analyzer;
	protected Schema _schema;

	public QueryConditionParser(Analyzer analyzer, Schema schema) {
		_analyzer = analyzer;
		_schema = schema;
	}
	
	public QueryConditionParser(Schema schema) {
		this(null,schema);
	}
	
	public void setAnalyzer(Analyzer analyzer){
		this._analyzer = analyzer;
	}
	
	public abstract Query parseQuery(Condition condition);
	
	public Sort parserSort(Condition condition) throws SortFieldNotValidException{
		Sort sort = condition.getSort();
		if(sort==null)
			return sort;
		SortField[] sortFields = sort.getSort();
		if(sortFields==null)
			return sort;
		
		int num = sortFields.length;
		for(int i=0;i<num;++i){
			SortField sortField = sortFields[i];
			if(sortField.isValid())
				continue;
			String name = sortField.getField();
			if(name==null||name.equals(""))
				throw new SortField.SortFieldNotValidException(sortField);
			else if(name.equals(SortField.SCORE)){
				sortField.setSourceType(SortField.DataSourceType.RESERVE);
				sortField.setType(SortField.DataType.SCORE);
			}
			else if(name.equals(SortField.DOC)){
				sortField.setSourceType(SortField.DataSourceType.RESERVE);
				sortField.setType(SortField.DataType.DOC);
			}
			else if(name.equals(SortField.UID)){
				sortField.setSourceType(SortField.DataSourceType.RESERVE);
				sortField.setType(SortField.DataType.UID);
			}
			else{
				if(this._schema.attributeSortFields.containsKey(name)){
					SortField schemaSortField = this._schema.attributeSortFields.get(name);
					if(sortField.getSourceType()==null)
						sortField.setSourceType(schemaSortField.getSourceType());
					if(sortField.getType()==null)
						sortField.setType(schemaSortField.getType());
					if(!sortField.isValid())
						throw new SortField.SortFieldNotValidException(sortField);
				}
				else{
					throw new SortField.SortFieldNotValidException(sortField);
				}
			}
		}
		return condition.getSort();
	}
}
