package com.renren.hadoop.cleandata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
//import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.renren.hadoop.util.IndexJobConfig;

public class CleanDataInputFormat extends FileInputFormat<LongWritable, Text> {

	static class CleanDataInputRecordReader implements
			RecordReader<LongWritable, Text> {
		private SequenceFile.Reader in;
		private int numRecord = 0;

		public CleanDataInputRecordReader(Configuration job, FileSplit split)
				throws IOException {
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(job);
			in = new SequenceFile.Reader(fs, path, job);
		}

		public void close() throws IOException {
			in.close();
		}

		public LongWritable createKey() {
			return new LongWritable();
		}

		public Text createValue() {
			return new Text();
		}

		public long getPos() throws IOException {
			return in.getPosition();
		}

		public float getProgress() throws IOException {
			return numRecord;
		}

		public boolean next(LongWritable key, Text value) throws IOException {

			if (in.next(key, value)) {
				numRecord++;
				return true;
			}
			return false;
		}
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		return new CleanDataInputRecordReader(job, (FileSplit) split);
	}

	public static void writeStateFile(JobConf conf) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		CleanDataInputFormat inFormat = new CleanDataInputFormat();

		FileStatus[] files = inFormat.listStatus(conf);
		Path old_parent = null;
		Path parent = null;

		int count = -1;
		//在kafka目录下打标签
		//方法比较笨，因为输入路径有*，得不到路径名，同时没有找到得到某Path下所有文件的方法。
		for (int j = 0; j < files.length; j++) {
			count++;
			parent = files[j].getPath().makeQualified(fs).getParent();
			if (!parent.equals(old_parent)) {

				if (old_parent != null
						&& old_parent.toString().contains("kafka")) {
					Path stateFile = new Path(old_parent,
							IndexJobConfig.KAFKA_STATE_FILE);
					FSDataOutputStream out = fs.create(stateFile, true);
					out.write(String.valueOf(count).getBytes("UTF-8"));
					out.close();
				}
				count = -1;
				old_parent = parent;
			}
		}
		if (parent.toString().contains("kafka")) {
			Path stateFile = new Path(parent, IndexJobConfig.KAFKA_STATE_FILE);
			FSDataOutputStream out = fs.create(stateFile, true);
			out.write(String.valueOf(count + 1).getBytes("UTF-8"));
			out.close();
		}

	}

	protected FileStatus[] listStatus(JobConf job) throws IOException {
		return super.listStatus(job);

	}
}
