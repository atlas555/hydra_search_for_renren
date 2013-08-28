package com.renren.search5.searcher.svc;

import junit.framework.Assert;
import org.junit.Test;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.renren.hydra.client.SearchType;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.parser.HydraQueryParser;
import com.renren.hydra.search.scorer.HydraScorer;
import com.renren.hydra.search.scorer.TermMatchInfo;
import com.renren.hydra.searcher.core.search.HydraIndexSearcher;

public class HydraIndexSearcherTest {
	@Test
	public void testNormal() throws Exception {
		try{	
			Directory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_30),
					IndexWriter.MaxFieldLength.UNLIMITED);
		
			Document doc1 = new Document();
			doc1.add(new Field("test", "人中华人民共人和国人",Field.Store.NO, Field.Index.ANALYZED));
			writer.addDocument(doc1);
			Document doc2 = new Document();
			doc2.add(new Field("test", "为人民平人平务平",Field.Store.NO, Field.Index.ANALYZED));
			writer.addDocument(doc2);
			Document doc3 = new Document();
			doc3.add(new Field("test", "世人界人人平类和平",Field.Store.NO, Field.Index.ANALYZED));
			writer.addDocument(doc3);

			IndexReader reader = writer.getReader();
			HydraIndexSearcher searcher = new HydraIndexSearcher(Schema.getInstance(),reader, 0,
				SearchType.All,null,null,null, null, null, null);
		
			HydraQueryParser parser = new HydraQueryParser(Version.LUCENE_30,
					"test", new StandardAnalyzer(Version.LUCENE_30));
			
			// test term query
			Query query = parser.parse("人");
			Term term = new Term("test", "人");
			Weight weight = query.createWeight(searcher);
			HydraScorer scorer = (HydraScorer)weight.scorer(reader, false, false);
			int docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 0, docid);

			TermMatchInfo[] matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 1, scorer.getMatchTermCount());
			TermMatchInfo info = matchInfos[0];
			Assert.assertEquals("wrong tf", 4, info.tf);

			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 1, docid);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 2, docid);
			matchInfos = scorer.getMatchInfos();
			info = matchInfos[0];
			Assert.assertEquals("wrong tf", 3, info.tf);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS, docid);


			// test boolean query
			query = parser.parse("为 平");
			Term term1 = new Term("test", "为");
			Term term2 = new Term("test", "平");
			weight = query.createWeight(searcher);
			scorer = (HydraScorer)weight.scorer(reader, false, false);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 1, docid);
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 2, scorer.getMatchTermCount());
			info = matchInfos[1];
			Assert.assertEquals("wrong term", "平", info.term.text());
			Assert.assertEquals("wrong tf", 3, info.tf);			
			docid = scorer.nextDoc();
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 1, scorer.getMatchTermCount());
			info = matchInfos[0];
			Assert.assertEquals("wrong term", "平", info.term.text());
			Assert.assertEquals("wrong tf", 2, info.tf);
			Assert.assertEquals("wrong doc id", 2, docid);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS, docid);


			// test boolean query
			query = parser.parse("人民 平");
			term1 = new Term("test", "人");
			term2 = new Term("test", "民");
			Term term3 = new Term("test", "平");
			weight = query.createWeight(searcher);
			scorer = (HydraScorer)weight.scorer(reader, false, false);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 0, docid);
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 2, scorer.getMatchTermCount());
			info = matchInfos[0];
			Assert.assertEquals("wrong tf", 4, info.tf);
			info = matchInfos[1];
			Assert.assertEquals("wrong tf", 1, info.tf);			
			docid = scorer.nextDoc();
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 3, scorer.getMatchTermCount());
			info = matchInfos[0];
			Assert.assertEquals("wrong tf", 2, info.tf);
			info = matchInfos[1];
			Assert.assertEquals("wrong tf", 1, info.tf);
			info = matchInfos[2];
			Assert.assertEquals("wrong tf", 3, info.tf);
			Assert.assertEquals("wrong doc id", 1, docid);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 2, docid);
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 1, scorer.getMatchTermCount());
			info = matchInfos[0];
			Assert.assertEquals("wrong tf", 2, info.tf);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS, docid);
		
			// test boolean query
			query = parser.parse("+人民 平");
			weight = query.createWeight(searcher);
			scorer = (HydraScorer)weight.scorer(reader, false, false);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 0, docid);
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 2, scorer.getMatchTermCount());
			info = matchInfos[0];
			Assert.assertEquals("wrong tf", 4, info.tf);
			info = matchInfos[1];
			Assert.assertEquals("wrong tf", 1, info.tf);			
			docid = scorer.nextDoc();
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 3, scorer.getMatchTermCount());
			info = matchInfos[0];
			Assert.assertEquals("wrong tf", 2, info.tf);
			info = matchInfos[1];
			Assert.assertEquals("wrong tf", 1, info.tf);
			info = matchInfos[2];
			Assert.assertEquals("wrong tf", 3, info.tf);
			Assert.assertEquals("wrong doc id", 1, docid);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS, docid);


			// test boolean query
			query = parser.parse("+人民 平 -务");
			weight = query.createWeight(searcher);
			scorer = (HydraScorer)weight.scorer(reader, false, false);
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", 0, docid);
			matchInfos = scorer.getMatchInfos();
			Assert.assertEquals("wrong match count", 2, scorer.getMatchTermCount());
			info = matchInfos[0];
			Assert.assertEquals("wrong tf", 4, info.tf);
			info = matchInfos[1];
			Assert.assertEquals("wrong tf", 1, info.tf);			
			docid = scorer.nextDoc();
			Assert.assertEquals("wrong doc id", DocIdSetIterator.NO_MORE_DOCS, docid);
	
		} catch (Exception e) {
			throw e;
			//Assert.assertEquals("wrong exception message", "", e.getMessage());	
		}
	}
}
