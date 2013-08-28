package com.renren.hadoop.cleandata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.renren.hydra.index.DocumentStateParser;

public class CleanDataReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	private static Logger logger = Logger.getLogger(CleanDataReducer.class);	
	private Text result = new Text();
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException,InterruptedException {

		boolean delete = false;
		List<JSONObject> valueList = new ArrayList<JSONObject>(); 
		// 一旦发现同一个id存在“_delete”记录，则丢弃该记录
		for(Text val:values) {
			String jsonStr = val.toString();
			JSONObject new_json = null;
			try {
				new_json = new JSONObject(jsonStr);
			} catch (JSONException e) {
				e.printStackTrace();
				continue;
			} 
			delete = DocumentStateParser.isDeleted(new_json);
			if (!delete) {
				valueList.add(new_json);
			}else
				break;
		}

		if (!delete) {
			for(JSONObject obj:valueList){
				result.set(obj.toString());
				context.write(key, result);
			}
		}else{
			logger.info("id " + key.toString() + " deleted!");
		}

	}

}
