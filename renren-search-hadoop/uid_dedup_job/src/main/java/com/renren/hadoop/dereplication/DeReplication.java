package com.renren.hadoop.dereplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.renren.hadoop.util.IndexJobConfig;
import com.renren.hadoop.util.PropertiesLoader;

/**
 * Generates the sampled split points, launches the job, and waits for it to
 * finish.
 * <p>
 * To run the program:
 * <b>bin/hadoop jar hadoop-examples-*.jar terasort in-dir out-dir</b>
 */
public class DeReplication  {
	
	private static Logger logger = Logger.getLogger(DeReplication.class);

  /**
   * 根据采样结果，把数据分发到不同的reducer上
   */
  static class DeReplicationPartitioner extends Partitioner<LongWritable,Text>{
	@Override
    public int getPartition(LongWritable key, Text value, int numPartitions) {
    	return (int) (key.get()%numPartitions);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
	if(args.length!=1){
		logger.info("usage DeReplication jobconfig");
			System.exit(-1);
	} 
	Configuration conf = PropertiesLoader.loadProperties(args[0]);
	
	logger.info("starting....");
	Job job = new Job(conf);
    
    SequenceFileInputFormat.addInputPath(job, new Path(conf.get(IndexJobConfig.DEDUP_INPUT_DIR)));
	SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get(IndexJobConfig.DEDUP_OUTPUT_DIR)));
	job.setJobName("DeReplication");
	job.setJarByClass(DeReplication.class);
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setPartitionerClass(DeReplicationPartitioner.class);
    job.setReducerClass(DeReplicationReducer.class);
    job.setMapperClass(DeReplicationMapper.class);
    job.setNumReduceTasks(conf.getInt(IndexJobConfig.DEDUP_NUM_REDUCER,1));
	
    int res = job.waitForCompletion(true) ? 0 : 1;
    System.exit(res);
  }
}
