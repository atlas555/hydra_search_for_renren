package com.renren.hadoop.cleandata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.chenlb.mmseg4j.analysis.SimpleAnalyzer;
import com.renren.hadoop.util.IndexJobConfig;
import com.renren.hadoop.util.JobUtil;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.util.IndexFlowFactory;

public class CleanDataMapper extends Mapper<LongWritable,Text,LongWritable,Text>{

	private static Logger logger = Logger.getLogger(CleanDataMapper.class);
	private DocumentProcessor processor;
	private LongWritable outKey;
	
	@Override
	public void map(LongWritable key, Text value,
			Context context) throws IOException,InterruptedException {
		long uid = 0L;
		try {
			uid = processor.getUid(new JSONObject(value.toString()));
			outKey.set(uid);
			context.write(outKey, value);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setup(Context context) {
		FileSystem fs = null;
		Configuration conf = context.getConfiguration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		String schemaUrl = conf.get(IndexJobConfig.SCHEMA_FILE_URL, "/user/jin.shang/newJob/build_index/schema.xml");
		Schema _schema = JobUtil.loadSchema(schemaUrl,fs);
		if(_schema == null){
			logger.error("[error]:schema is null");
			System.exit(-1);
		}
		//拷贝hdfs的词典文件到本地
		processor = (DocumentProcessor) IndexFlowFactory.createDocumentProcessor(_schema, new SimpleAnalyzer());
		if(processor==null){
			logger.error("create document processor error");
			System.exit(-1);
		}
		outKey = new LongWritable();
		logger.info("build index mapper setup ok");
	}

}
