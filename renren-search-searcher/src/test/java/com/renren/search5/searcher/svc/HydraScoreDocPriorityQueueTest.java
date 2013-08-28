package com.renren.search5.searcher.svc;

import junit.framework.Assert;
import org.junit.Test;

import org.apache.lucene.search.ScoreDoc;

import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.HydraSortScoreDocPriorityQueue;

public class HydraScoreDocPriorityQueueTest {
	@Test
	public void testNormal() {
		HydraSortScoreDocPriorityQueue<HydraScoreDoc> pq = new HydraSortScoreDocPriorityQueue<HydraScoreDoc>(5);
		HydraScoreDoc doc1 = new HydraScoreDoc(2, 3.2f);
		pq.insertWithOverflow(doc1);
		HydraScoreDoc doc2 = new HydraScoreDoc(6, 1.0f);
		pq.insertWithOverflow(doc2);
		HydraScoreDoc doc3 = new HydraScoreDoc(9, 3.2f);
		pq.insertWithOverflow(doc3);
		Assert.assertEquals("queue size is wrong", 3, pq.size());

		HydraScoreDoc doc4 = new HydraScoreDoc(15, 0.9f);
		pq.insertWithOverflow(doc4);
		HydraScoreDoc doc5 = new HydraScoreDoc(19, 6.5f);
		pq.insertWithOverflow(doc5);
		HydraScoreDoc doc6 = new HydraScoreDoc(25, 3.5f);
		pq.insertWithOverflow(doc6);

		Assert.assertEquals("queue size is wrong", 5, pq.size());
		Assert.assertEquals("top score is wrong", 1.0f, pq.top().score);
		Assert.assertEquals("top docid is wrong", 6, pq.top().doc);

		ScoreDoc res0 = pq.pop();
		Assert.assertEquals("first score is wrong", 1.0f, res0.score);
		Assert.assertEquals("first docid is wrong", 6, res0.doc);
		ScoreDoc res1 = pq.pop();
		Assert.assertEquals("second score is wrong", 3.2f, res1.score);
		Assert.assertEquals("second docid is wrong", 2, res1.doc);
		ScoreDoc res2 = pq.pop();
		Assert.assertEquals("third score is wrong", 3.2f, res2.score);
		Assert.assertEquals("third docid is wrong", 9, res2.doc);
		ScoreDoc res3 = pq.pop();
		Assert.assertEquals("fourth score is wrong", 3.5f, res3.score);
		Assert.assertEquals("fourth docid is wrong", 25, res3.doc);
		ScoreDoc res4 = pq.pop();
		Assert.assertEquals("fifth score is wrong", 6.5f, res4.score);
		Assert.assertEquals("fifth docid is wrong", 19, res4.doc);

		Assert.assertEquals("queue size is wrong", 0, pq.size());
	}
	
	@Test
	public void TestSort(){
		HydraSortScoreDocPriorityQueue<HydraScoreDoc> pq = new HydraSortScoreDocPriorityQueue<HydraScoreDoc>(3);
		HydraScoreDoc s1 = new HydraScoreDoc(1,(float) 1.0000001);
		HydraScoreDoc s2 = new HydraScoreDoc(2,(float) 1.0);
		HydraScoreDoc s3 = new HydraScoreDoc(3,(float) 3.0);
		HydraScoreDoc s4 = new HydraScoreDoc(4,(float) 4.0);
		HydraScoreDoc s5 = new HydraScoreDoc(5,(float) 5.0);
		HydraScoreDoc s6 = new HydraScoreDoc(6,(float) 10.0);
		HydraScoreDoc s7 = new HydraScoreDoc(7,(float) 2.0);
		
		pq.insertWithOverflow(s1);
		pq.insertWithOverflow(s2);
		pq.insertWithOverflow(s3);
		pq.insertWithOverflow(s4);
		pq.insertWithOverflow(s5);
		pq.insertWithOverflow(s6);
		pq.insertWithOverflow(s7);
		
		/*
		 * 输出应该为  4, 4.0 , 5 5.0 6 10.0
		 */
		ScoreDoc s = pq.pop();
		Assert.assertEquals("wrong docid", 4, s.doc);
		s = pq.pop();
		Assert.assertEquals("wrong docid", 5, s.doc);
		s = pq.pop();
		Assert.assertEquals("wrong docid", 6, s.doc);
	
		
		
		HydraSortScoreDocPriorityQueue<HydraScoreDoc> pq2 = new HydraSortScoreDocPriorityQueue<HydraScoreDoc>(3,true);
		pq2.insertWithOverflow(s1);
		pq2.insertWithOverflow(s2);
		pq2.insertWithOverflow(s3);
		pq2.insertWithOverflow(s4);
		pq2.insertWithOverflow(s5);
		pq2.insertWithOverflow(s6);
		pq2.insertWithOverflow(s7);
		
		/*
		 * 输出应该为  7, 2.0 , 1 1.0 2 1.00001
		 */
		s = pq2.pop();
		Assert.assertEquals("wrong docid", 7, s.doc);
		s = pq2.pop();
		Assert.assertEquals("wrong docid", 1, s.doc);
		s = pq2.pop();
		Assert.assertEquals("wrong docid", 2, s.doc);
	}
}
