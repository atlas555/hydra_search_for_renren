package com.renren.hadoop.dereplication;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;
import com.renren.hydra.index.DefaultVersionComparator;

public class DeReplicationReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	private Text result = new Text();
	private DefaultVersionComparator versionComparator = new DefaultVersionComparator();	

	@Override
	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		JSONObject json = null;
		String maxVersion=null;
		
		//保留version最大的
		for(Text val : values) {
			String jsonStr = val.toString();
			JSONObject new_json = null;
			try {
				new_json = new JSONObject(jsonStr);
			} catch (JSONException e) {
				e.printStackTrace();
				continue;
			}
			String new_version=null;
			if(new_json.has("version"))
				new_version = new_json.optString("version",null);
			if(versionComparator.compare(maxVersion, new_version)<=0){
				maxVersion=new_version;
				json=new_json;
			}
		}
		result.set(json.toString());
		context.write(key, result);
	}
}