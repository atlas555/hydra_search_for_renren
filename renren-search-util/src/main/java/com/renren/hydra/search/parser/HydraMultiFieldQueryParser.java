package com.renren.hydra.search.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.apache.lucene.queryParser.ParseException;

public class HydraMultiFieldQueryParser extends HydraQueryParser
{
	protected String[] fields;

	public HydraMultiFieldQueryParser(Version matchVersion, String[] fields, 
			Analyzer analyzer) 
	{
		super(matchVersion, null, analyzer);
		this.fields = fields;
	}
  
	protected Query getFieldQuery(String field, String queryText, 
			int slop) throws ParseException 
	{
		if (field == null) {
			List<BooleanClause> clauses = new ArrayList<BooleanClause>();
			for (int i = 0; i < fields.length; i++) {
				Query q = super.getFieldQuery(fields[i], queryText);
				if (q != null) {
					applySlop(q, slop);
					clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
				}
			}
			if (clauses.size() == 0)  // happens for stopwords
				return null;
			return getBooleanQuery(clauses, true);
		}
		
		Query q = super.getFieldQuery(field, queryText);
		applySlop(q, slop);
		return q;
	}

	private void applySlop(Query q, int slop) {
		if (q instanceof PhraseQuery) {
			((PhraseQuery) q).setSlop(slop);
    	} else if (q instanceof MultiPhraseQuery) {
    		((MultiPhraseQuery) q).setSlop(slop);
    	}
	}
  

	protected Query getFieldQuery(String field, String queryText) 
		throws ParseException 
	{
		return getFieldQuery(field, queryText, 0);
	}

	protected Query getFuzzyQuery(String field, String termStr, 
			float minSimilarity) throws ParseException
	{
		if (field == null) {
			List<BooleanClause> clauses = new ArrayList<BooleanClause>();
			for (int i = 0; i < fields.length; i++) {
				clauses.add(new BooleanClause(getFuzzyQuery(fields[i], 
								termStr, minSimilarity),
				BooleanClause.Occur.SHOULD));
			}
			return getBooleanQuery(clauses, true);
		}
				    
		return super.getFuzzyQuery(field, termStr, minSimilarity);
	}

	protected Query getPrefixQuery(String field, String termStr) 
		throws ParseException
	{
		if (field == null) {
			List<BooleanClause> clauses = new ArrayList<BooleanClause>();
			for (int i = 0; i < fields.length; i++) {
				clauses.add(new BooleanClause(getPrefixQuery(fields[i], termStr),
						BooleanClause.Occur.SHOULD));
			}
			return getBooleanQuery(clauses, true);
		}
				       
		return super.getPrefixQuery(field, termStr);
	}

	protected Query getWildcardQuery(String field, String termStr) 
		throws ParseException 
	{
		if (field == null) {
			List<BooleanClause> clauses = new ArrayList<BooleanClause>();
			for (int i = 0; i < fields.length; i++) {
				clauses.add(new BooleanClause(getWildcardQuery(fields[i], termStr),
						BooleanClause.Occur.SHOULD));
			}
			return getBooleanQuery(clauses, true);
		}
			      
		return super.getWildcardQuery(field, termStr);
	}

	protected Query getRangeQuery(String field, String part1, 
			String part2, boolean inclusive) throws ParseException 
	{
		if (field == null) {
			List<BooleanClause> clauses = new ArrayList<BooleanClause>();
			for (int i = 0; i < fields.length; i++) {
				clauses.add(new BooleanClause(
							getRangeQuery(fields[i], part1, part2, inclusive),
							BooleanClause.Occur.SHOULD));
				}
				return getBooleanQuery(clauses, true);
		}
			      
		return super.getRangeQuery(field, part1, part2, inclusive);
	}
}
