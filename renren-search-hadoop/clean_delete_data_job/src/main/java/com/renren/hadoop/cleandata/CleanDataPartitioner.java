package com.renren.hadoop.cleandata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CleanDataPartitioner extends Partitioner<LongWritable, Text> {
	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		return (int)(key.get() % numPartitions); 
	}
}