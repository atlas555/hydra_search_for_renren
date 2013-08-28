package com.renren.hydra.thirdparty.zkmanager2;


public class ZkConfig {
  protected static final int ZK_TIMEOUT = 5000;
  protected static String ZK_ADDRESS = "10.11.17.31:2181,10.11.17.32:2181,10.11.17.33:2181,10.11.17.34:2181,10.11.17.35:2181";
  protected static final String ZK_LOCALHOST_ADDRESS = "localhost:2181";
  protected static final String LOCK = "/globallock";
  protected static final String ACL_TYPE = "digest";
  protected static final String ACL_INFO = "search:renrensearch";
}
