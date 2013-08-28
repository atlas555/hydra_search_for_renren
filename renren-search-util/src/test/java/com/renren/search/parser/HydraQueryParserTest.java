package com.renren.search.parser;

import junit.framework.Assert;
import org.junit.Test;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.renren.hydra.search.parser.HydraBooleanQuery;
import com.renren.hydra.search.parser.HydraPhraseQuery;
import com.renren.hydra.search.parser.HydraQueryParser;
import com.renren.hydra.search.parser.HydraTermQuery;

public class HydraQueryParserTest {
	private HydraQueryParser parser;
	public HydraQueryParserTest() {
		parser = new HydraQueryParser(Version.LUCENE_30,
				"test", new StandardAnalyzer(Version.LUCENE_30));
	}
	
	@Test
	public void testSimpleTermQuery() {
		try {
			Query query = parser.parse("中");
			Assert.assertTrue("query type is wrong",
					query instanceof HydraTermQuery);
		} catch (Exception e) {
			Assert.assertTrue("exception catch", false);
		}
	}

	@Test
	public void testSimpleBooleanQuery1() {
		try {
			parser.setDefaultOperator(QueryParser.AND_OPERATOR);

			Query query = parser.parse("中 国");
			Assert.assertTrue("query type is wrong",
					query instanceof HydraBooleanQuery);
			HydraBooleanQuery booleanQuery = (HydraBooleanQuery)query;
			BooleanClause[] clauses = booleanQuery.getClauses();
			Assert.assertEquals("wrong clause size", 2, clauses.length);
			Assert.assertEquals("wrong occur", BooleanClause.Occur.MUST,
					clauses[0].getOccur());
			Assert.assertTrue("query type is wrong",
					clauses[0].getQuery() instanceof HydraTermQuery);
			TermQuery termQuery1 = (TermQuery)clauses[0].getQuery();
			Assert.assertTrue("term value is wrong",
					termQuery1.getTerm().text().equals("中"));

		} catch (Exception e) {
			Assert.assertTrue("exception catch", false);
		}
	}

	
	@Test
	public void testSimpleBooleanQuery2() {
		try {
			parser.setDefaultOperator(QueryParser.OR_OPERATOR);

			String qStr = "+中 国";
			Query query = parser.parse(qStr);
			Assert.assertTrue("query type is wrong",
					query instanceof HydraBooleanQuery);
			HydraBooleanQuery booleanQuery = (HydraBooleanQuery)query;
			BooleanClause[] clauses = booleanQuery.getClauses();
			Assert.assertEquals("wrong clause size", 2, clauses.length);
			Assert.assertEquals("wrong occur", BooleanClause.Occur.MUST,
					clauses[0].getOccur());
			Assert.assertTrue("query type is wrong",
					clauses[0].getQuery() instanceof HydraTermQuery);
			TermQuery termQuery1 = (TermQuery)clauses[0].getQuery();
			Assert.assertTrue("term value is wrong",
					termQuery1.getTerm().text().equals("中"));
			Assert.assertEquals("wrong occur", BooleanClause.Occur.SHOULD,
					clauses[1].getOccur());
			TermQuery termQuery2 = (TermQuery)clauses[1].getQuery();
			Assert.assertTrue("term value is wrong",
					termQuery2.getTerm().text().equals("国"));
		} catch (Exception e) {
			Assert.assertTrue("exception catch", false);
		}
	}

	@Test
	public void testSimplePhraseQuery() {
		try {
			Query query = parser.parse("中国");
			Assert.assertTrue("query type is wrong", 
					query instanceof HydraPhraseQuery);
			HydraPhraseQuery phraseQuery = (HydraPhraseQuery)query;
			Term[] terms = phraseQuery.getTerms();
			Assert.assertEquals("wrong term count", 2, terms.length);
		} catch (Exception e) {
			Assert.assertTrue("exception catch", false);
		}
	}

	@Test
	public void testComplexPhraseQuery() {
		try {
			parser.setDefaultOperator(QueryParser.AND_OPERATOR);

			Query query = parser.parse("中国 人民");
			Assert.assertTrue("query type is wrong",
					query instanceof HydraBooleanQuery);
			HydraBooleanQuery booleanQuery = (HydraBooleanQuery)query;
			BooleanClause[] clauses = booleanQuery.getClauses();
			Assert.assertEquals("wrong clause size", 2, clauses.length);
			Assert.assertEquals("wrong occur", BooleanClause.Occur.MUST,
				clauses[0].getOccur());
			Assert.assertTrue("query type is wrong",
					clauses[0].getQuery() instanceof HydraPhraseQuery);
			Assert.assertTrue("query type is wrong",
					clauses[1].getQuery() instanceof HydraPhraseQuery);
		} catch (Exception e) {
			Assert.assertTrue("exception catch", false);
		}
	}
}
