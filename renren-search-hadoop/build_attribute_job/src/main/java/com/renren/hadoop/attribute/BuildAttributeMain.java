package com.renren.hadoop.attribute;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import com.renren.hadoop.util.IndexJobConfig;
import com.renren.hadoop.util.PropertiesLoader;
import com.renren.hydra.attribute.DefaultOnlineAttributeData;

public class BuildAttributeMain {

	public static void main(String[] args) throws Exception {
		if(args.length!=1){
			System.out.println("usage BuildAttributeMain jobconfig");
			System.exit(-1);
		} 
		Configuration conf = PropertiesLoader.loadProperties(args[0]);
		
		//convert to new API
		Job job = new Job(conf);
		job.setJobName("BuildAttribute");
		job.setNumReduceTasks(conf.getInt(IndexJobConfig.NUM_PARTITION,1));
		job.setJarByClass(BuildAttributeMain.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DefaultOnlineAttributeData.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		SequenceFileInputFormat.addInputPath(job, new Path(conf.get(IndexJobConfig.BUILD_ATTRIBUTE_INPUT_DIR)));
		SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get(IndexJobConfig.BUILD_ATTRIBUTE_OUTPUT_DIR)));
		
		job.setMapperClass(BuildAttributeMapper.class);
		job.setPartitionerClass(BuildAttributePartitioner.class);
		job.setReducerClass(BuildAttributeReducer.class);
		
		
		Path dicDirPathOnHDFS = new Path(conf.get(IndexJobConfig.ANALYZER_DIC_DIR, "/user/chuanyu.ban/jin.shang/tmp/index/dic"));
		DistributedCache.addCacheFile(dicDirPathOnHDFS.toUri(), job.getConfiguration());
		int res = job.waitForCompletion(true) ? 0 : 1;
		System.exit(res);		
	}
}
