package com.renren.hadoop.cleandata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.renren.hadoop.util.IndexJobConfig;
import com.renren.hadoop.util.PropertiesLoader;

public class CleanData {
	private static Logger logger = Logger.getLogger(CleanData.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args.length!=1){
			logger.info("usage CkeanData jobconfig");
			System.exit(-1);
		} 
		Configuration conf = PropertiesLoader.loadProperties(args[0]);
				
		logger.info("starting");
		Job job = new Job(conf);
		FileSystem fs = FileSystem.get(conf);
		int numReducer = 1;
		
		String kafkaDataDir = conf.get(IndexJobConfig.KAFKA_DATA_DIR,"/offline/kafka");
		int numPartition = conf.getInt(IndexJobConfig.NUM_PARTITION, 1);
		int count = 0;
		for(int i = 0; i < numPartition; i++) {
			String dir = kafkaDataDir + "/part_" + i;
			logger.info("dir: " + dir);
			Path partPath = new Path(dir);
			if(!fs.exists(partPath)){
				logger.error("path is not exists:"+dir);
				System.exit(-1);
			}
			FileStatus[] files = fs.listStatus(partPath);
			for(FileStatus file: files) {
				logger.info("filename: " + file.toString());
				if(file.getPath().getName().contains("version"))
					count++;
			}
			for(int j = 0; j < count; j++) {
				SequenceFileInputFormat.addInputPath(job, new Path(dir + "/" + j));
				System.out.println("filedir: " + dir + "/" + j);
			}
			
			Path stateFile = new Path(dir, IndexJobConfig.KAFKA_STATE_FILE);
			FSDataOutputStream out = fs.create(stateFile, true);
			out.write(String.valueOf(count-1).getBytes("UTF-8"));
			out.close();
			count = 0;
		}
		
		
		String historyDirs = conf.get(IndexJobConfig.HISTORY_DATA_DIRS);
		if(null!=historyDirs && !historyDirs.trim().equals(""))
			SequenceFileInputFormat.addInputPath(job, new Path(historyDirs));

		logger.info(SequenceFileInputFormat.getInputPaths(job));
		SequenceFileOutputFormat.setOutputPath(job, new Path(conf.get(IndexJobConfig.CLEAN_DATA_OUTPUT_DIR,"/offline/index")));
	
		numReducer = conf.getInt(IndexJobConfig.CLEAN_DATA_NUM_REDUCER, 1);
		job.setJobName("CleanData");
		job.setJarByClass(CleanData.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(CleanDataMapper.class);
		job.setPartitionerClass(CleanDataPartitioner.class);
		job.setReducerClass(CleanDataReducer.class);
		job.setNumReduceTasks(numReducer);
	
		int res = job.waitForCompletion(true) ? 0 : 1;
		System.exit(res);
	}

}