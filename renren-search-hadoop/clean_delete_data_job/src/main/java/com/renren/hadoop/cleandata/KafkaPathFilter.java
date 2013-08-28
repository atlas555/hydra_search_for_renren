package com.renren.hadoop.cleandata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
//输入路径下的含有"version"的文件不参与 map reduce
public class KafkaPathFilter implements PathFilter{
	String skip = "version";
	
	public boolean accept(Path path) {
		
		if(path.getName().contains(skip))
			return false;
		
		return true;
	}
}
