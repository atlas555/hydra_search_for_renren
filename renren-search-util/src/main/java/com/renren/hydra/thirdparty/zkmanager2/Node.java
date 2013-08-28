package com.renren.hydra.thirdparty.zkmanager2;

import java.io.Serializable;


public interface Node extends Serializable{
	public String business();
	public String getPath();
	public String getAbsolutePath();
	public boolean isEmpty();
	public double getQPS();
	public void setQPS(double QPS);
}