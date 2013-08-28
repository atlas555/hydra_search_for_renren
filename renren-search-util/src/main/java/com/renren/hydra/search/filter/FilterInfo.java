package com.renren.hydra.search.filter;

import java.io.Serializable;


public class FilterInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String filterName;
	private String filterCondition;

	public FilterInfo(String name,String condition){
		this.filterName = name;
		this.filterCondition = condition;
	}
	

	public String getFilterName() {
		return filterName;
	}
	public void setFilterName(String filterName) {
		this.filterName = filterName;
	}

	public String getFilterCondition() {
		return filterCondition;
	}

	public void setFilterCondition(String filterCondition) {
		this.filterCondition = filterCondition;
	}

	
	public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != getClass())
            return false;

        FilterInfo other = (FilterInfo) obj;
        if(this.filterName==null && other.filterName==null && this.filterCondition==null && other.filterCondition==null)
        	return true;
        if(this.filterName.equals(other.filterName) && this.filterCondition.equals(other.filterCondition))
        	return true;
        return false;
    }
	
	//filterName 可能相同，filterCondition 不太可能相同
	public int hashCode() {
		return this.filterCondition.hashCode();
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(this.filterName);
		sb.append("\t");
		sb.append(this.filterCondition);
		return sb.toString();
	}
}
