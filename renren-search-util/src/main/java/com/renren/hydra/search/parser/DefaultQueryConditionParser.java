package com.renren.hydra.search.parser;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Version;

import com.renren.hydra.client.Condition;
import com.renren.hydra.client.SearchType;
import com.renren.hydra.config.schema.Schema;

import com.renren.hydra.search.parser.QueryConditionParser;

public class DefaultQueryConditionParser extends QueryConditionParser {
	private static final Logger logger = Logger
			.getLogger(DefaultQueryConditionParser.class);

	private String[] _fields;

	public String[] getFields() {
		return this._fields;
	}

	public DefaultQueryConditionParser(Schema schema) {
		this(null, schema);
	}

	public DefaultQueryConditionParser(Analyzer analyzer, Schema schema) {
		super(analyzer, schema);
		String[] searchFields = schema.getScoreFiledNames();
		int size = searchFields.length;
		_fields = new String[size];
		int id = 0;
		for (id = 0; id < size; ++id) {
			_fields[id] = searchFields[id];
		}
	}

	@Override
	public Query parseQuery(Condition condition) {
		String qStr = condition.getQuery();
		if ((qStr == null || qStr.isEmpty()) ) {
			return new MatchAllDocsQuery();
		} 
		HydraQueryParser parser = new HydraMultiFieldQueryParser(
				Version.LUCENE_30, _fields, _analyzer);
		parser.setDefaultOperator(condition.getOperator());
		Query query = null;
		try {
			query = parser.parse(qStr);
		} catch (ParseException e) {
			logger.error(e);
		}


		return query;

	}
}
