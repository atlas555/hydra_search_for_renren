package com.renren.hydra.attribute;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONObject;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OfflineAttributeManager;
import com.renren.hydra.util.HydraLong;

public class ShareOfflineAttributeManager extends OfflineAttributeManager {

	public ShareOfflineAttributeManager(String path) throws Exception{
		super(path);
	}
	
	@Override
	protected void loadData(String filename) throws Exception {
		logger.info("start to load offline attribute data from file " + filename);
		SequenceFile.Reader reader = null;
		LongWritable key = new LongWritable();
		Writable value = new Text();
		Path path = new Path(filename);

		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			while (reader.next(key, value)) {
				OfflineAttributeData data = new ShareOfflineAttributeData();
				JSONObject json = new JSONObject(value.toString());
				if(data.init(json))
					_offlineAttrDataTable.put(new HydraLong(key.get()), data);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (reader != null) {
				IOUtils.closeStream(reader);
			}
		}

		logger.info("finish load offline attribute data from file " + filename);

	}

}
