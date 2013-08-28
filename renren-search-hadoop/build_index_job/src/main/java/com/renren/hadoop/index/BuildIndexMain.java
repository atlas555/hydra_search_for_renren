package com.renren.hadoop.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.renren.hadoop.util.IndexJobConfig;
import com.renren.hadoop.util.PropertiesLoader;

public class BuildIndexMain {
	private static final Log logger = LogFactory.getLog(BuildIndexMain.class);

	public static void main (String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("BuildIndexMain: <confFile> <jobName>");
			System.exit(0);
		}
		Configuration conf = PropertiesLoader.loadProperties(args[0]);
		Job job = new Job(conf, args[1]);
		
		job.setNumReduceTasks(conf.getInt(IndexJobConfig.NUM_PARTITION,1));
		job.setJarByClass(BuildIndexMain.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(conf.get(IndexJobConfig.BUILD_INDEX_INPUT_DIR)));
		SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get(IndexJobConfig.BUILD_INDEX_OUTPUT_DIR)));

		job.setMapperClass(BuildIndexMapper.class);
		job.setPartitionerClass(BuildIndexPartitioner.class);
		job.setReducerClass(BuildIndexReducer.class);
		
		Path dicDirPathOnHDFS = new Path(conf.get(IndexJobConfig.ANALYZER_DIC_DIR, "/user/chuanyu.ban/jin.shang/tmp/index/dic"));
		DistributedCache.addCacheFile(dicDirPathOnHDFS.toUri(), job.getConfiguration());
		
		
		int res = job.waitForCompletion(true) ? 0 : 1;
		System.exit(res);
	}
}
