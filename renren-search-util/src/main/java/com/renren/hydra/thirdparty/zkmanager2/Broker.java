package com.renren.hydra.thirdparty.zkmanager2;

import java.io.Serializable;


public class Broker implements Node, Serializable {

	private static final long serialVersionUID = 1L;

	private static final String PARENT_PATH = "/search2/broker";

	private String ip;
	private String port;
	private String business;
	private boolean isAlive;
	private double QPS;

	public Broker() {
		this(null, null, null, false, 0.0);
	}

	public Broker(String ip, String port, String business) {
		this(ip, port, business, true, 0.0);
	}

	public Broker(String ip, String port, String business, boolean isAlive,
			Double QPS) {
		this.ip = ip;
		this.port = port;
		this.isAlive = isAlive;
		this.business = business;
		this.QPS = QPS;
	}

	@Override
	public String getPath() {
		return ip + "_" + port + "_" + business;
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
		return (ip == null || ip.isEmpty()) || (port == null || port.isEmpty())
				|| (business == null || business.isEmpty());
	}

	@Override
	public String business() {
		return business;
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

	public boolean isAlive() {
		return isAlive;
	}

	public void setAlive(boolean isAlive) {
		this.isAlive = isAlive;
	}

	public double getQPS() {
		return QPS;
	}

	public void setQPS(double qPS) {
		QPS = qPS;
	}

	@Override
	public String toString() {
		return getPath();
	}
}
