package com.renren.hydra.util;

import java.util.Set;
import java.util.Map;
import java.net.URI;

import org.apache.log4j.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import com.renren.hydra.attribute.OnlineAttributeData;

public class SequenceFileUtil {
	private static Logger logger = Logger.getLogger(SequenceFileUtil.class);

	/*
	 * Map<Long, AttributeData> ——> SequenceFile
	 */
	public static void write(String fileName, Map<HydraLong, OnlineAttributeData> map)
			throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fileName), conf);
		Path path = new Path(fileName);
		logger.info("create sequence file writer for file: " + fileName);
		SequenceFile.Writer writer = null;
		LongWritable lwt = new LongWritable();
		try {
			Set<HydraLong> keys = map.keySet();
			for (HydraLong key : keys) {
				OnlineAttributeData attributeData = map.get(key);
				if (writer == null) {
					writer = SequenceFile.createWriter(fs, conf, path,
							LongWritable.class, attributeData.getClass(),CompressionType.NONE);
				}
				lwt.set(key.get());
				writer.append(lwt, attributeData);
			}

		} finally {
			IOUtils.closeStream(writer);
		}
	}
	
	public static void readDelUIDs(String fileName, Map<Long, Boolean> map)
			throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fileName), conf);
		Path path = new Path(fileName);
		logger.info("create sequence file reader for file: " + fileName);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			LongWritable key = new LongWritable();
			while (reader.next(key)) {
				map.put(key.get(), true);
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	
}
