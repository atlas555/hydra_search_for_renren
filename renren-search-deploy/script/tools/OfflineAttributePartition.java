import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.json.JSONException;
import org.json.JSONObject;


public class OfflineAttributePartition {
	public static int JsonFilePartition(String filename,int partitionNum,String outputFolder,String idField) throws IOException{
		File file = new File(filename);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		String line = "";
		int count = 0;
		int errCount=0;
		int[] pCounts = new int[partitionNum];
		for(int i=0;i<partitionNum;++i)
			pCounts[i]=0;
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filename), conf);
		SequenceFile.Writer[] writers = initWriters(fs,conf,partitionNum,outputFolder);
		
		
		while(true){
			line = bufferedReader.readLine();
			if(line==null)
				break;

			JSONObject obj =null;
			try{
				obj = new JSONObject(line);
				Long key = obj.optLong(idField);
				LongWritable lwt = new LongWritable();
				lwt.set(key);
				int index=(int) (key % partitionNum);
				writers[index].append(lwt, new Text(obj.toString()));
				++pCounts[index];
			}catch(JSONException e){
				e.printStackTrace();
				errCount++;
			}finally{
				++count;
				if(count%10000==0)
					System.out.println("processed "+count+" records");
			}
		}
		
		System.out.println("total record num :"+count+" error record num is :"+errCount);
		for(int i=0;i<partitionNum;i++){
			IOUtils.closeStream(writers[i]);
			System.out.println(pCounts[i]+" records for partition "+i);
		}

		
		
		return 0;
		
	}
	
	public static SequenceFile.Writer[] initWriters(FileSystem fs, Configuration conf,int partitionNum,String outputFolder) throws IOException{
		SequenceFile.Writer[] writers = new SequenceFile.Writer[partitionNum];
		int i=0;
		for(i=0;i<partitionNum;++i){
			Path path = new Path(outputFolder+File.separator+i);
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
					LongWritable.class, Text.class);
			writers[i] = writer;
		}
		return writers;
	}
	
	public static int SequenceFilePartition(String filename,int partitionNum,String outputFolder) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filename), conf);
		SequenceFile.Reader reader = null;
		Path path = new Path(filename);
		int cnt=0;
		
		int[] pCounts = new int[partitionNum];
		for(int i=0;i<partitionNum;++i)
			pCounts[i]=0;
		
		if(!fs.exists(path)){
			System.out.println("input file: "+path+" not exists");
			return -1;
		}
		
		Path outPath = new Path(outputFolder);
		if(!fs.exists(outPath)){
			System.out.println("output dir: "+path+" not exists");
			return -1;
		}
		SequenceFile.Writer[] writers = initWriters(fs,conf,partitionNum,outputFolder);
		reader = new SequenceFile.Reader(fs, path, conf);
		LongWritable key = new LongWritable();
		Text value = new Text();
		
		while (reader.next(key, value)) {
				int index = (int) (key.get() % partitionNum);
				writers[index].append(key, value);
				++pCounts[index];
				++cnt;
				if(cnt%10000==0)
					System.out.println("processed record: "+cnt);
		}
		System.out.println("process record: "+cnt);
		IOUtils.closeStream(reader);
		for(int i=0;i<partitionNum;i++){
			IOUtils.closeStream(writers[i]);
			System.out.println(pCounts[i]+" records for partition "+i);
		}
		return 0;
	}
	
	public static void main(String[] args){
		if(args.length<4){
			System.out.println("usage OfflineAttributeParition inputformat([sequence|json] inputfile partitionNum outputFolder [idfield]");
			System.exit(-1);
		}
		String inputFormat=args[0].toLowerCase();
		String inputFile =args[1];
		int numPartition = Integer.parseInt(args[2]);
		String outputFolder = args[3];
		
		System.out.println("InputFormat: "+inputFormat+ "\n inputPath: "+inputFile+"\n numPartition: "+numPartition+"\n outputFolder: "+outputFolder);
		
		int ret=0;
		
		if(inputFormat.equals("sequence")){
			try {
				ret=SequenceFilePartition(inputFile,numPartition,outputFolder);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}else if(inputFormat.equals("json")){
			if(args.length!=5){
				System.out.println("usage OfflineAttributeParition json inputfile partitionNum outputFolder idfield");
				System.exit(-1);
			}
			String idField = args[4];
			System.out.println("idField:"+idField);
			try {
				ret=JsonFilePartition(inputFile,numPartition,outputFolder,idField);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}else{
			System.out.println("input format: "+inputFormat+" doesn't supported, usage sequence or json");
			System.exit(-1);
		}
		System.exit(ret);
	}
}
