package com.renren.hydra.thirdparty.zkmanager2;

import java.io.Serializable;

public class Searcher implements Node, Serializable {
	private static final long serialVersionUID = 1L;
	private static final String PARENT_PATH = "/search2/searcher";

	private  String ip;
	private  String port;
	private  String business;
	private  String partitions;
	private  int[] partitionsArray;
	private  double QPS;

	boolean isAlive;

	public Searcher() {
		this(null, null, null, null);
	}

	public Searcher(String ip, String port, String business, int[] partitions) {
		this(ip, port, business, partitions, true, 0.0);
	}

	public void generatePartitionString(){
		StringBuffer sb = new StringBuffer();
		int cnt = this.partitionsArray.length;
		for(int i=0;i<cnt;i++){
			sb.append("-");
			sb.append(this.partitionsArray[i]);
		}
		if(sb.length()>1)
			this.partitions = sb.substring(1);
		else
			this.partitions = null;
	}
	
	public Searcher(String ip, String port, String business, int[] partitions,
			boolean isAlive, double QPS) {
		this.ip = ip;
		this.port = port;
		this.business = business;
		this.partitionsArray = partitions;
		generatePartitionString();
		this.isAlive = isAlive;
		this.QPS = QPS;
	}

	@Override
	public String getPath() {
		StringBuffer buffer = new StringBuffer(getPrefix());
		for (int i = 0; i < partitionsArray.length; i++) {
			buffer.append("_").append(partitionsArray[i]);
		}

		return buffer.toString();
	}

	public static String getParentPath() {
		return PARENT_PATH;
	}

	@Override
	public String getAbsolutePath() {
		return getParentPath() + "/" + getPath();
	}

	@Override
	public boolean isEmpty() {
		boolean result = (ip == null || ip.isEmpty())
				|| (port == null || port.isEmpty())
				|| (business == null || business.isEmpty())
				|| (partitions == null);
		return result;
	}

	public String getPrefix() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(ip).append("_").append(port).append("_").append(business);

		return buffer.toString();
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getBusiness() {
		return business;
	}

	public void setBusiness(String business) {
		this.business = business;
	}

	public int[] getPartitions() {
		return partitionsArray;
	}

	public void setPartitions(int[] partitions) {
		this.partitionsArray = partitions;
	}

	public boolean isAlive() {
		return isAlive;
	}

	public void setAlive(boolean isAlive) {
		this.isAlive = isAlive;
	}

	@Override
	public String business() {
		return business;
	}

	@Override
	public String toString() {
		return getPath();
	}

	@Override
	public double getQPS() {
		return this.QPS;
	}

	@Override
	public void setQPS(double QPS) {
		this.QPS = QPS;
	}
	
	/*private void writeObject(ObjectOutputStream oos) throws IOException {
		ObjectOutputStream.PutField fields = oos.putFields();
		fields.put("ip", this.ip);
		fields.put("port", this.port);
		fields.put("business", this.business);
        fields.put("partitions", this.partitions);
        fields.put("isAlive", this.isAlive);
        fields.put("QPS", this.QPS);
        //fields.put("partitionsArray", this.partitionsArray);
        oos.writeFields();
    }*/

	/*private void readObject(ObjectInputStream ois) throws IOException,
            ClassNotFoundException {
    	GetField field = ois.readFields();
    	this.ip = (String) field.get("ip", null);
    	this.business = (String) field.get("business", null);
    	this.port = (String) field.get("port", null);
    	this.isAlive = field.get("isAlive", false);
    	this.QPS = field.get("QPS", 0.0);
    	this.partitions=(String) field.get("partitions", null);
    	this.partitionsArray = (int[]) field.get("partitionsArray", null);
    	
    	Object paritionsObj = field.get("partitions", null);
 
    	if(paritionsObj==null)
    		this.partitionsArray = new int[0];
    	else if(paritionsObj instanceof String){
    		String partitionsStr = ((String)paritionsObj);
    		if(partitionsStr==null||partitionsStr.equals("")){
    			this.partitionsArray = new int[0];
    		}else{
    			String[] partitionArray = ((String)paritionsObj).split("-");
    			int cnt = partitionArray.length;
    			this.partitionsArray = new int[cnt];
    			for(int i=0;i<cnt;i++){
    				try{
    					int partitionId = Integer.valueOf(partitionArray[i]);
    					this.partitionsArray[i] = partitionId;
    				}catch (Exception e){
    					logger.error("convert 2 integer error for partition string:"+partitionArray[i]);
    				}
    			}
    		}
    	}else if(paritionsObj instanceof int[]){
    		this.partitionsArray = (int[]) paritionsObj;
    	}else{
    		this.partitionsArray = new int[0];
    	}
    	generatePartitionString();
    }*/
}
