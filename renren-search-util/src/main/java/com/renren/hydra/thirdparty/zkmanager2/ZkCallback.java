package com.renren.hydra.thirdparty.zkmanager2;

import java.util.List;

public interface ZkCallback {
	public void handleChildChange(String parentPath, List<String> currentChilds);
	public void handleDataChange(String dataPath, Object data);
	public void handleDataDeleted(String dataPath);
}
