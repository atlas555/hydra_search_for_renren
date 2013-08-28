package com.renren.hadoop.attribute;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.json.JSONException;
import org.json.JSONObject;

import com.renren.hadoop.util.JobUtil;
import com.renren.hydra.attribute.DefaultOnlineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.util.IndexFlowFactory;

public class BuildAttributeReducer extends
    Reducer<LongWritable, Text, LongWritable, DefaultOnlineAttributeData> {
	private static Logger logger = Logger.getLogger(BuildAttributeReducer.class);
	private DocumentProcessor processor;
	private Configuration iconf;
	private LocalFileSystem localFs;
	private FileSystem fs;
	private FSDataOutputStream out;
	private SequenceFile.Writer writer;
	private String fileDir;
	private Schema _schema;
	private Analyzer dpAnalyzer;

	@Override
	public void setup(Context context) {
		System.out.println("configure");
		iconf = context.getConfiguration();
		try {
			fs = FileSystem.get(iconf);
			localFs = FileSystem.getLocal(iconf);
			
			String schemaUrl = iconf.get("schema.file.url", "/user/jin.shang/newJob/build_index/schema.xml");
			_schema = JobUtil.loadSchema(schemaUrl, fs);
			if(_schema==null){
				System.err.println("[error]:schema is null");
				System.exit(-1);
			}
			dpAnalyzer = JobUtil.createAnalyzer(iconf,_schema);
			
			if(dpAnalyzer == null){
				System.err.println("create analyzer for ducumentprocessor error");
				System.exit(-1);
			}
			//拷贝hdfs的词典文件到本地
			processor = (DocumentProcessor) IndexFlowFactory.createDocumentProcessor(_schema, dpAnalyzer);
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(-1);
		}

	}

	public void close() throws IOException {
		logger.info("close");
		if(writer != null)
			writer.close();
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values,
		  Context context) throws IOException {
		
		for(Text val : values) {
			String jsonStr = val.toString();
			JSONObject originalJson;

				try {
					originalJson = new JSONObject(jsonStr);
					if(originalJson == null) {
						logger.info("originalJson is null");
						continue;
					}
					OnlineAttributeData attrData = processor.process(originalJson);
					if(attrData == null) {
						logger.info("attrData is null");
						continue;
					}
					context.write(key, (DefaultOnlineAttributeData)attrData);
				} catch (JSONException e) {
					logger.error("jsonexcepiton");
					e.printStackTrace();
				}
				catch (Exception e) {
					logger.error("exception"+e);
					e.printStackTrace();
				}				
			
		}
	}
}
