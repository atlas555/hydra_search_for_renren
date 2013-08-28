package com.renren.hadoop.index;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

//import com.renren.hadoop.index.module.JsonSchemaInterpreterForBuildJob;
import com.renren.hadoop.index.module.ZoieAnalyzerForBuildJob;
import com.renren.hadoop.util.IndexJobConfig;
import com.renren.hadoop.util.JobUtil;
import com.renren.hadoop.util.LuceneIndexFileNameFilter;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
//import com.renren.hydra.index.DefaultJsonSchemaInterpreter;
import com.renren.hydra.index.DocumentProcessor;
import com.renren.hydra.index.HydraZoieIndexable;

import com.renren.hydra.util.IndexFlowFactory;

//import com.chenlb.mmseg4j.analysis.SimpleAnalyzer;

public class BuildIndexReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	private static Logger logger = Logger.getLogger(BuildIndexReducer.class);
	
	private DocumentProcessor processor;
	private IndexWriter writer;
	private Configuration iconf;
	private LocalFileSystem localFs;
	private FileSystem fs;
	private ZoieIndexableInterpreter interpreter;
	private Analyzer analyzer;
	private Path localIndexPath;
	private Path hadoopIndexPath;
	private Schema _schema;
	private Analyzer dpAnalyzer;
	
	public static final String uidTermVal = "_UID";
	
	@Override
	public void setup(Context context) {
		//获得confuguration
		iconf = context.getConfiguration();
		
		try {
			//获得hdfs
			fs = FileSystem.get(iconf);
			//操作本地文件
			localFs = FileSystem.getLocal(iconf);
			
			//加载schema
			String schemaUrl = iconf.get(IndexJobConfig.SCHEMA_FILE_URL, "/user/jin.shang/newJob/build_index/schema.xml");
			_schema = JobUtil.loadSchema(schemaUrl,fs);
			if(_schema == null){
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
			
			interpreter = buildInterpreter(_schema);
			
			//生成存储索引文件的本地目录
			String indexDirStr = "./index." + System.currentTimeMillis();
			localIndexPath = new Path(indexDirStr);
			if (localFs.exists(localIndexPath)) {
				localFs.delete(localIndexPath);
			}
			//生成分词器
			analyzer = buildAnalyzer();
			//生成indexWriter对象
			writer = createWriter(indexDirStr);
			//生成hdfs存放索引文件的路径
			String taskId = iconf.get("mapred.task.id");
			logger.info("taskId: " + taskId);
			String hadoopIndexdir = iconf.get(IndexJobConfig.INDEX_DATA_DIR, "/user/jin.shang/newJob/build_index/indexData");
			String hadoopDir = hadoopIndexdir + Path.SEPARATOR + JobUtil.getTaskCode(taskId); 
			logger.info("hadoopDir: " + hadoopDir);

			hadoopIndexPath = new Path(hadoopDir);
			
		} catch(Exception e) {
			
		}		
	}
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) 
		throws IOException ,InterruptedException {
		for (Text text : values) {
			try {
				Text js = text;
				String jsonStr = js.toString();
				JSONObject originalJson = new JSONObject(jsonStr);
				OnlineAttributeData attrData = processor.process(originalJson);
				HydraZoieIndexable indexable = (HydraZoieIndexable) interpreter.convertAndInterpret(originalJson);
				IndexingReq[] idxReqs = indexable.buildIndexingReqs();
				Document doc = idxReqs[0].getDocument();
				StringBuilder sb = new StringBuilder();
				long uid = indexable.getUID();
				sb.append(uidTermVal).append("|").append(Long.toString(uid));
				Field field = new Field(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD,
					sb.toString(), Field.Store.NO, Field.Index.ANALYZED_NO_NORMS,
					Field.TermVector.NO);
				doc.add(field);
				writer.addDocument(doc, analyzer);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//将缓冲区内容写入磁盘
		writer.optimize();
		writer.close();
		//将产生的索引文件移动到hdfs
		moveFromLocalToHadoop();

		if (localFs.exists(localIndexPath)) {
			localFs.delete(localIndexPath);
		}

		// 创建index.directory文件
		String kafkaDir = iconf.get(IndexJobConfig.KAFKA_DATA_DIR, "/user/jin.shang/newJob/KAFKA_DATA");
		Path kafkaDirPath = new Path(kafkaDir);
		
		//kafka path 不存在，退出
		if(!fs.exists(kafkaDirPath))
			return;
		
		FileStatus[] files = fs.listStatus(new Path(kafkaDir));
		int partitionNum = files.length;
		
		String dstDir = iconf.get(IndexJobConfig.INDEX_DATA_DIR,
				"/user/jin.shang/newJob/build_index/indexData");
		if (!fs.exists(new Path(dstDir + Path.SEPARATOR + "0" + Path.SEPARATOR
				+ "index.directory"))) {
			for (int i = 0; i < partitionNum; i++) {
				String fileName = kafkaDir + Path.SEPARATOR + "part_" + i
						+ Path.SEPARATOR + IndexJobConfig.KAFKA_STATE_FILE;
				System.out.println("fileName: " + fileName);
				Path file = new Path(fileName);
				if (fs.exists(file)) {
					FSDataInputStream in = fs.open(file);
					byte[] b = new byte[1024];
					in.read(b);
					String numStr = new String(b);
					in.close();
					System.out.println("numStr: " + numStr);
					int num = Integer.parseInt(numStr.trim(), 10);
					String versionFile = kafkaDir + Path.SEPARATOR + "part_"
							+ i + Path.SEPARATOR + "version." + num;
					System.out.println("versionfile: " + versionFile);
					FSDataInputStream versionIn = fs
							.open(new Path(versionFile));
					byte[] b2 = new byte[1024];
					versionIn.read(b2);
					String version = new String(b2);
					System.out.println("version: " + version);
					versionIn.close();
					Path stateFile = new Path(dstDir + Path.SEPARATOR + i,
							"index.directory");
					FSDataOutputStream out = fs.create(stateFile, true);
					out.write(version.trim().getBytes("UTF-8"));
					out.close();
				} else {
					System.out.println("file don't exit!");
				}
			}
		}

	}
	
	private ZoieIndexableInterpreter buildInterpreter(Schema schema) throws Exception {
		logger.info("create Interpreter");
		if(schema == null) {
			System.out.println("schema is null!");
			System.exit(-1);
		}
		return (ZoieIndexableInterpreter)IndexFlowFactory.createJsonSchemaInterpreter(_schema);
	}

	//生成分词器
	private Analyzer buildAnalyzer(){
		return new ZoieAnalyzerForBuildJob();
	}

	//创建IndexWriter对象
	private IndexWriter createWriter(String dir) throws IOException {
		logger.info("create lucene index writer with dir : " + dir);
		IndexWriter writer = new IndexWriter(FSDirectory.open(new File(dir)), 
				null, new KeepOnlyLastCommitDeletionPolicy(), 
				MaxFieldLength.UNLIMITED);

		if (iconf != null) {
			int maxFieldLength = iconf.getInt("max.field.length", -1);
			if (maxFieldLength > 0) {
				logger.info("set MaxFieldLength: " + maxFieldLength);
				writer.setMaxFieldLength(maxFieldLength);
			}
		}

		int maxBufferSizeMb = iconf.getInt("rambuffersize.mb", 250);
		logger.info("set RAMBufferSizeMB: " + maxBufferSizeMb);
		writer.setRAMBufferSizeMB(maxBufferSizeMb);

		logger.info("set UseCompoundFile: " + false);
		writer.setUseCompoundFile(false);
		return writer;
	}
	
	private void moveFromLocalToHadoop() throws IOException {
		System.out.println("start move index from local to hadoop");
		FileStatus[] fileStatus = localFs.listStatus(localIndexPath,
				LuceneIndexFileNameFilter.getFilter());
		localFs.setVerifyChecksum(false);
		for (int i = 0; i < fileStatus.length; i++) {
			Path path = fileStatus[i].getPath();
			String name = path.getName();
			System.out.println("copy file " + name);
			fs.copyFromLocalFile(path, new Path(hadoopIndexPath, name));
		}
		logger.info("finish move index from local to hadoop");
	}
}
