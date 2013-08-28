package com.renren.hadoop.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BuildIndexPartitioner extends Partitioner<LongWritable, Text> {
	@Override
	public int getPartition(LongWritable uid, Text content, int numReduceTasks) {
		return (int)(uid.get() % numReduceTasks); 
	}
}
