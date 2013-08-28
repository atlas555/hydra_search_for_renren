package com.renren.hydra.search.filter;

import java.io.Serializable;

public class Filter implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private FilterInfo[] filterInfos;

	public Filter(){
		this.filterInfos=null;
	}
	
	public Filter(FilterInfo[] filterInfos){
		this.filterInfos=filterInfos;
	}
	
	public Filter(FilterInfo filterInfo){
		this.filterInfos = new FilterInfo[]{filterInfo};
	}
	
	public FilterInfo[] getFilterInfos() {
		return filterInfos;
	}
	
	public void addFilterInfo(FilterInfo filterInfo){
		if(this.filterInfos==null)
			this.filterInfos = new FilterInfo[]{(filterInfo)};
		else{
			int cnt = this.filterInfos.length;
			FilterInfo[] tmpInfos = new FilterInfo[cnt+1];
			System.arraycopy(this.filterInfos, 0, tmpInfos, 0,cnt);
			tmpInfos[cnt]=filterInfo;
			this.filterInfos=null;
			this.filterInfos=tmpInfos;
			tmpInfos=null;
		}
	}
	
	public void addFilterInfos(FilterInfo[] filterInfos){
		if(this.filterInfos==null)
			this.filterInfos = filterInfos;
		else{
			int cnt = this.filterInfos.length;
			int incrCnt = filterInfos.length;
			FilterInfo[] tmpInfos = new FilterInfo[cnt+incrCnt];
			System.arraycopy(this.filterInfos, 0, tmpInfos, 0,cnt);
			System.arraycopy(filterInfos, 0, tmpInfos, cnt, incrCnt);
			this.filterInfos=null;
			this.filterInfos=tmpInfos;
			tmpInfos=null;
		}
	}

	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		if(this.filterInfos==null)
			sb.append("null");
		else{
			int cnt = this.filterInfos.length;
			for(int i=0;i<cnt;++i){
				sb.append("fiter:");
				sb.append(i);
				sb.append("\t");
				sb.append(this.filterInfos[i].toString());
				sb.append("\n");
			}
		}
		return sb.toString();
	}
}
