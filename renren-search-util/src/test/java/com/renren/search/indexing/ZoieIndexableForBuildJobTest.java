//package com.renren.search.indexing;
//
//import java.util.Map;
//import java.util.HashMap;
//import java.util.Set;
//import java.util.HashSet;
//
//import junit.framework.Assert;
//import org.junit.Test;
//
//import org.json.JSONObject;
//
//import org.apache.lucene.analysis.Analyzer;
//import org.apache.lucene.analysis.standard.StandardAnalyzer;
//import org.apache.lucene.document.Document;
//import org.apache.lucene.document.Field.Index;
//import org.apache.lucene.document.Field.Store;
//import org.apache.lucene.document.Field.TermVector;
//import org.apache.lucene.util.Version;
//import org.apache.lucene.store.Directory;
//import org.apache.lucene.store.RAMDirectory;
//import org.apache.lucene.store.FSDirectory;
//import org.apache.lucene.index.IndexWriter;
//import org.apache.lucene.index.IndexReader;
//import org.apache.lucene.index.Term;
//import org.apache.lucene.index.TermPositions;
//import org.apache.lucene.search.IndexSearcher;
//import org.apache.lucene.search.TopDocs;
//import org.apache.lucene.search.Query;
//import org.apache.lucene.search.TermQuery;
//
//import com.renren.searchengine.util.IndexFlowFactory;
//import com.renren.searchengine.config.schema.IndexSchema;
//import com.renren.searchengine.config.schema.MetaType;
//import com.renren.searchengine.documentProcessor.ShareDocumentProcessor;
//import com.renren.searchengine.index.JsonSchemaInterpreterForBuildJob;
//import com.renren.searchengine.index.ZoieIndexableForBuildJob;
//import com.renren.searchengine.attribute.ShareOnlineAttributeData;
////
//import proj.zoie.api.indexing.AbstractZoieIndexable;
//import proj.zoie.api.indexing.ZoieIndexable;
//import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;
////
//public class ZoieIndexableForBuildJobTest {
//	private class FakeSchema extends IndexSchema {
//		private Map<String, FieldDefinition> indexFieldDefMap;
//		private Set<String> fieldsNotAnalyzed;
//		private Map<Integer, String> _idFieldMap;
//		private Map<String, String> _flowNodeMap;
//
//	public FakeSchema() {
//			indexFieldDefMap = new HashMap<String, FieldDefinition>();
//	
//			FieldDefinition def = new FieldDefinition();
//			def.store = Store.NO;
//			def.index = Index.ANALYZED_NO_NORMS;
//			def.tv = TermVector.NO;
//			def.fieldType = MetaType.String;
//			
//			indexFieldDefMap.put("title", def);
//			indexFieldDefMap.put("summary", def);
//
//			fieldsNotAnalyzed = new HashSet<String>();
//			fieldsNotAnalyzed.add("title");
//			fieldsNotAnalyzed.add("summary");
//			_idFieldMap = new HashMap<Integer, String>();
//			_idFieldMap.put(0, "title");
//			_idFieldMap.put(1, "summary");
//			_flowNodeMap = new HashMap<String, String>();
//			_flowNodeMap.put(IndexFlowFactory.DocumentProcessor, "com.xiaonei.searchengine.documentProcessor.ShareDocumentProcessor");
//		}
//
//		public Map<String, FieldDefinition> getIndexfieldDefMap() {
//			return indexFieldDefMap;
//		}
//
//		public Set<String> getAvailableForSortFields() {
//			return fieldsNotAnalyzed;
//		}
//
//		public String getUidField() {
//			return "url_md5"; 
//		}
//		
//		public String getFlowNodeClass(String module) {
//			return _flowNodeMap.get(module);
//		}
//		
//		@Override
//		public Set<String> getIndexFields() {
//			return fieldsNotAnalyzed;
//		}
//	
//	}
//
//	private Directory directory;
//
//	public static long bytesToLong(byte[] bytes){
//		return ((long)(bytes[7] & 0xFF) << 56) | 
//			((long)(bytes[6] & 0xFF) << 48) | 
//			((long)(bytes[5] & 0xFF) << 40) | 
//			((long)(bytes[4] & 0xFF) << 32) | 
//			((long)(bytes[3] & 0xFF) << 24) | 
//			((long)(bytes[2] & 0xFF) << 16) | 
//			((long)(bytes[1] & 0xFF) <<  8) |  
//			(long)(bytes[0] & 0xFF);
//	}
//
//	@Test
//	public void test() throws Exception {
//		try {
//			JSONObject json = new JSONObject();
//			json.put("title", "中国人");
//			json.put("summary", "人民万岁");
//			json.put("other field", "为人民服务");
//			String uidStr = "11111111111111111111111111111111";
//			json.put("url_md5", uidStr);
//			json.put("creation_date", "1980-10-12 20:12:10");
//
//			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
//			FakeSchema schema = new FakeSchema();
//			ShareDocumentProcessor processor = new ShareDocumentProcessor(analyzer, schema);
//			System.out.println(null==schema);	
//			ShareOnlineAttributeData data = (ShareOnlineAttributeData)processor.process(json);
//			
//			JsonSchemaInterpreterForBuildJob interpreter = 
//				new JsonSchemaInterpreterForBuildJob(schema);
//
//			ZoieIndexable indexable = interpreter.convertAndInterpret(json);
//			IndexingReq[] reqs = indexable.buildIndexingReqs();
//
//			Document document = reqs[0].getDocument();
//
//			directory = new RAMDirectory();
//			IndexWriter indexWriter = new IndexWriter(directory, 
//					new StandardAnalyzer(null), 
//					IndexWriter.MaxFieldLength.UNLIMITED);
//			indexWriter.addDocument(document);
//			indexWriter.close();
//
//			IndexSearcher searcher = new IndexSearcher(directory);
//			Term t = new Term("title", "中");
//			Query query = new TermQuery(t);
//			TopDocs topDocs = searcher.search(query, 10);
//			Assert.assertTrue("doc count is wrong", topDocs.totalHits == 1);
//
//			IndexReader indexReader = searcher.getIndexReader();
//			Term uid_term = new Term(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD,
//					ZoieIndexableForBuildJob.uidTermVal);
//			TermPositions tp = indexReader.termPositions(uid_term);
//			while (tp.next()) {
//				int doc = tp.doc();
//				Assert.assertTrue("doc id is wrong", doc == 0);
//				byte[] payloadBuffer = new byte[8];
//				tp.nextPosition();
//				tp.getPayload(payloadBuffer, 0);
//				long uid = bytesToLong(payloadBuffer);
//				long expectUid = ShareDocumentProcessor.getHashValue(uidStr);
//				Assert.assertEquals("uid is wrong", uid, expectUid);
//			}
//		} catch (Exception e) {
//			throw e;
//			//Assert.assertTrue(e.getMessage(), false);
//		}
//	}
//}
