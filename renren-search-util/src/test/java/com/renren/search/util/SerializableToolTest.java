package com.renren.search.util;

import java.util.Set;
import java.util.HashSet;
import junit.framework.Assert;
import org.junit.Test;

import org.apache.lucene.util.Version;

import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.parser.HydraMultiFieldQueryParser;
import com.renren.hydra.search.parser.HydraPhraseQuery;
import com.renren.hydra.util.SerializableTool;

public class SerializableToolTest {
	@Test
	public void testWriteAndRead() throws Exception {
		try {
			HydraRequest request = new HydraRequest();
			Set<Integer> partitions = new HashSet<Integer>();
			partitions.add(1);
			partitions.add(5);

			request.setPartitions(partitions);
			HydraMultiFieldQueryParser parser = new HydraMultiFieldQueryParser(Version.LUCENE_30, 
					null, null);
			HydraPhraseQuery query = new HydraPhraseQuery(parser);
			request.setQuery(query);

			byte[] data = SerializableTool.objectToBytes(request);
			Assert.assertTrue("data is empty", data.length != 0);

			HydraRequest actualRequest = (HydraRequest)SerializableTool.bytesToObject(data);
			Assert.assertTrue("request is null", actualRequest != null);
			Set<Integer> newParts = actualRequest.getPartitions();
			Assert.assertTrue("item count is not equal", 
					actualRequest.getPartitions().size() == partitions.size());
		} catch (Exception e) {
			throw e;
			//Assert.assertTrue(e.getMessage(), false);
		}
	}
}
