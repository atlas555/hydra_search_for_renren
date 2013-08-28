package com.renren.search.util;

import org.junit.Test;


public class SequenceFileUtilTest {
	@Test
	public void testWriteAndRead() throws Exception {
		/*try {
			ConcurrentHashMap<Long, OnlineAttributeData> mapWrite = 
				new ConcurrentHashMap<Long, OnlineAttributeData>();
			
			ShareOnlineAttributeData data1 = new ShareOnlineAttributeData();
			data1.summaryTtf = 1;
			data1.titleTtf = 5;
			mapWrite.put(3415L, data1);
			

			ShareOnlineAttributeData data2 = new ShareOnlineAttributeData();
			data2.summaryTtf = 2;
			data2.titleTtf = 3;
			mapWrite.put(5892L, data2);

			String fileName = "/tmp/SequenceFileUtilTest_test";
			SequenceFileUtil.write(fileName, mapWrite);

			String confDir="/home/xiaobing/javaworkspace/zootest/src/";
			String schemaFileName = "IndexSchema.xml";
			IndexSchema schema = new IndexSchema();
			schema.initSchema(confDir, schemaFileName);
			
			ConcurrentHashMap<Long, OnlineAttributeData> mapRead = 
				new ConcurrentHashMap<Long, OnlineAttributeData>();
			SequenceFileUtil.read(fileName, mapRead, schema);

			Assert.assertEquals("size is wrong", 2, mapRead.size());	
			Assert.assertEquals("value is wrong", 1, ((ShareOnlineAttributeData)mapRead.get(3415L)).summaryTtf);
			Assert.assertEquals("value is wrong", 5, ((ShareOnlineAttributeData)mapRead.get(3415L)).titleTtf);
			Assert.assertEquals("value is wrong", 2, ((ShareOnlineAttributeData)mapRead.get(5892L)).summaryTtf);
			Assert.assertEquals("value is wrong", 3,((ShareOnlineAttributeData) mapRead.get(5892L)).titleTtf);
		} catch (Exception e) {
			throw e;
			//Assert.assertTrue(e.getMessage(), false);
		}*/
	}
}
