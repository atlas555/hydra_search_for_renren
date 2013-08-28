package com.renren.hydra.search.sort;

import java.io.Serializable;

public class Sort implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private boolean needScore;
	private SortField[] sortFields;
	
	public Sort(){
		this.sortFields = null;
		this.needScore = false;
	}
	
	public Sort(SortField[] sortFields){
		this(sortFields,false);
	}
	
	public Sort(SortField[] sortFields, boolean needScore){
		this.sortFields = sortFields;
		this.needScore = needScore;
	}
	
	public Sort(SortField sortField){
		this(sortField,false);
	}
	
	public Sort(SortField sortField,boolean needScore){
		setSort(sortField);
		this.needScore = needScore;
	}
	
	public void setSort(SortField sortField){
		this.sortFields = new SortField[]{sortField};
	}
	
	public SortField[] getSort(){
		return this.sortFields;
	}

	public boolean isNeedScore() {
		return needScore;
	}

	public void setNeedScore(boolean needScore) {
		this.needScore = needScore;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(this.needScore?"need score ":" ");
		if(this.sortFields==null)
			sb.append("num of sortfield:0");
		else{
			sb.append("num of sortfield:"+this.sortFields.length+" {");
			int num = this.sortFields.length;
			sb.append(this.sortFields[0].toString());
			for(int i=1;i<num;++i){
				sb.append(",\t"+this.sortFields[i].toString());
			}
			sb.append("}");
		}
		return sb.toString();
	}
}
