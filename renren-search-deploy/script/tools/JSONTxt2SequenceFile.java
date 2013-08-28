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

/*
 * 将Json文件转换成SequenceFile
 * 输入：
 *    包含JSON串的文件，一行一个json字符串，一般通过JSONObject.toString 生成
 *    id 域，用于解析SeqenceFile record 的key
 * 输出：
 *    SequenceFile
 */
public class JSONTxt2SequenceFile {
	
	public static void txt2sequence(String txtFile,String idField,String outputfile) throws IOException{
		File file = new File(txtFile);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);

		String line = "";
		int count = 0;
		int errNum=0;

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(outputfile), conf);
		Path path = new Path(outputfile);
		System.out.println("create sequence file writer for file: " + outputfile);
		SequenceFile.Writer writer = null;
		 
		writer = SequenceFile.createWriter(
					fs, conf, path, LongWritable.class, Text.class);
			
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
				writer.append(lwt, new Text(obj.toString()));
			}catch(JSONException e){
				++errNum;
				System.out.println("error:"+line+"\n:"+count);
			}finally{
				++count;
				if(count%10000==0)
					System.out.println("processed "+count+" records");
			}
		}
		IOUtils.closeStream(writer);
		System.out.println("total record num :"+count+" error record num is :"+errNum);
	}
	
	public static void main(String[] args) {
		if(args.length!=3){
			System.out.println("usage java JSONTxt2SequenceFile jsontxtfile idfield outputfile");
			System.exit(-1);
		}
		try {
			txt2sequence(args[0],args[1],args[2]);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.exit(0);
	}
	
}
