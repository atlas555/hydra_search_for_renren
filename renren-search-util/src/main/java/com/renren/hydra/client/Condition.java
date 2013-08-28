package com.renren.hydra.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParser.Operator;

import com.renren.hydra.search.filter.AttributeSearchFilter;
import com.renren.hydra.search.filter.Filter;
import com.renren.hydra.search.filter.FilterInfo;
import com.renren.hydra.search.filter.AttributeSearchFilter.AttributeFilterCondition.AttributeFilterOperator;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.search.sort.SortField;

public class Condition implements Serializable {

	private static final long serialVersionUID = 1L;

	private String queryStr;
	private boolean needExplain;
	private Operator op;
	private Sort sort;
	private boolean fillAttribute;
	private boolean highlight;
	private Filter filter;
	
	private SearchType searchType;
	private int userId;
	private Map<String, String> userInfo;
	private Map<Integer, Map<Integer, Short>> friendsInfo;
	//fast version friend info
	private Map<Integer, byte[] > friendsInfoBytes;
	private String originalQueryString;	
	
	public Condition() {
		this.needExplain = false;                                           
	    queryStr = "";
		op = QueryParser.AND_OPERATOR;                                      
		this.sort = null;
		this.fillAttribute = true;                                          
		this.filter = null;
		this.highlight=true;   
			
		userId = 0;
		this.searchType = SearchType.All;
		this.userInfo = null;
		this.originalQueryString = "";
		this.friendsInfoBytes = null;
		this.friendsInfo = null;
	}
	
	public String getOriginalQueryString() {
		return this.originalQueryString;
	}
	
	public void setOriginalQueryString(String queryString) {
		this.originalQueryString = queryString;
	}
	
	public SearchType getSearchType() {
		return this.searchType;
	}

	public void setSearchType(SearchType searchType) {
		this.searchType = searchType;
	}

	public Map<String, String> getUserInfo() {
		if(userInfo==null)
			userInfo = new HashMap<String,String>();
		return userInfo;
	}

	public void setUserInfo(Map<String, String> userInfo) {
                this.userInfo = userInfo;
        }

	public int getUserId() {
		return this.userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public Map<Integer, Map<Integer, Short>> getFriendsInfo() {
		return this.friendsInfo;
	}

	public void setFriendsInfo(Map<Integer, Map<Integer, Short>> friendsInfo) {
		this.friendsInfo = friendsInfo;
	}
	
	public Map<Integer, byte[] > getFriendsInfoBytes() {
		return this.friendsInfoBytes;
	}

	public void setFriendsInfoBytes(Map<Integer, byte[] > friendsInfo) {
		this.friendsInfoBytes = friendsInfo;
	}

	public boolean isHighlight() {
		return this.highlight;
	}

	public void setHighlight(boolean highlight) {
		this.highlight = highlight;
	}

	public Operator getOperator() {
		return this.op;
	}

	public boolean isFillAttribute() {
		return this.fillAttribute;
	}

	public void setFillAttribute(boolean fillAttribute) {
		this.fillAttribute = fillAttribute;
	}

	public void setOperator(Operator _op) {
		this.op = _op;
	}

	public void setUnionSearch() {
		setOperator(QueryParser.OR_OPERATOR);
	}

	public void setQuery(String query) {
		this.queryStr = query;
	}

	public void setNeedExplain() {
		this.needExplain = true;
	}

	public String getQuery() {
		return QueryParser.escape(this.queryStr);
	}

	public boolean isNeedExplain() {
		return this.needExplain;
	}

	public String getOrginalQuery() {
		return this.queryStr;
	}

	public Sort getSort() {
		return this.sort;
	}

	public void setSort(Sort sort) {
		this.sort = sort;
	}

	public void setSort(SortField[] sortFields) {
		this.setSort(sortFields, false);
	}

	public void setSort(SortField[] sortFields, boolean needScore) {
		this.setSort(new Sort(sortFields, needScore));
	}

	public Filter getFilter() {
		return this.filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}
	public void setFilter(FilterInfo filterInfo){
		this.setFilter(new Filter(filterInfo));
	}
	
	public void setFilter(FilterInfo[] filterInfos){
		this.setFilter(new Filter(filterInfos));
	}
	
	public void addFilterInfo(FilterInfo filterInfo){
		if(this.filter==null)
			this.filter = new Filter(filterInfo);
		else
			this.filter.addFilterInfo(filterInfo);
	}
	
	public void addFilterInfos(FilterInfo[] filterInfos){
		if(this.filter==null)
			this.filter = new Filter(filterInfos);
		else
			this.filter.addFilterInfos(filterInfos);
	}
	
	public void addAttributeFilter(String attributeName,AttributeFilterOperator operator,String strValue){
		this.addFilterInfo(new FilterInfo(AttributeSearchFilter.class.getSimpleName(),attributeName+" "+operator.toString()+" "+strValue));
	}
	
	public void addAttributeFilters(String[] attributeNames, AttributeFilterOperator[] operators, String[] strValues){
		int cnt = attributeNames.length;
		if(cnt!=operators.length||cnt!=strValues.length)
			return;
		FilterInfo[] filterInfos = new FilterInfo[cnt];
		for(int i=0;i<cnt;++i)
			filterInfos[i] = new FilterInfo(AttributeSearchFilter.class.getSimpleName(),attributeNames[i]+" "+operators[i].toString()+" "+strValues[i]);
		this.addFilterInfos(filterInfos);
	}

	public void addEqualAttributeFilter(String attributeName,String strValue){
		this.addAttributeFilter(attributeName, AttributeFilterOperator.EQ, strValue);
	}
	
	public void addNotEqualAttributeFilter(String attributeName,String strValue){
		this.addAttributeFilter(attributeName, AttributeFilterOperator.NE, strValue);
	}
	
	public void addGreaterThanAttributeFilter(String attributeName,String strValue){
		this.addAttributeFilter(attributeName, AttributeFilterOperator.GT, strValue);
	}
	
	public void addGreaterOrEqualAttributeFilter(String attributeName,String strValue){
		this.addAttributeFilter(attributeName, AttributeFilterOperator.GE, strValue);
	}
	
	public void addLessThanAttributeFilter(String attributeName,String strValue){
		this.addAttributeFilter(attributeName, AttributeFilterOperator.LT, strValue);
	}
	
	public void addLessOrEqualAttributeFilter(String attributeName,String strValue){
		this.addAttributeFilter(attributeName, AttributeFilterOperator.LE, strValue);
	}
	
	public void addClosedIntervalAttributeFilter(String attributeName,String minValue,String maxValue){
		String[] attributeNames = new String[]{attributeName,attributeName};
		AttributeFilterOperator[] operators = new AttributeFilterOperator[]{AttributeFilterOperator.GE,AttributeFilterOperator.LE};
		String[] values = new String[]{minValue,maxValue};
		this.addAttributeFilters(attributeNames, operators, values);
	}
	
	public void addLeftClosedIntervalAttributeFilter(String attributeName,String minValue,String maxValue){
		String[] attributeNames = new String[]{attributeName,attributeName};
		AttributeFilterOperator[] operators = new AttributeFilterOperator[]{AttributeFilterOperator.GE,AttributeFilterOperator.LT};
		String[] values = new String[]{minValue,maxValue};
		this.addAttributeFilters(attributeNames, operators, values);
	}
	
	public void addRightClosedIntervalAttributeFilter(String attributeName,String minValue,String maxValue){
		String[] attributeNames = new String[]{attributeName,attributeName};
		AttributeFilterOperator[] operators = new AttributeFilterOperator[]{AttributeFilterOperator.GT,AttributeFilterOperator.LE};
		String[] values = new String[]{minValue,maxValue};
		this.addAttributeFilters(attributeNames, operators, values);
	}
	
	public void addOpenIntervalAttributeFilter(String attributeName,String minValue,String maxValue){
		String[] attributeNames = new String[]{attributeName,attributeName};
		AttributeFilterOperator[] operators = new AttributeFilterOperator[]{AttributeFilterOperator.GT,AttributeFilterOperator.LT};
		String[] values = new String[]{minValue,maxValue};
		this.addAttributeFilters(attributeNames, operators, values);
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("query string:");
		sb.append(this.queryStr);
		sb.append("\t operator:");
		sb.append(this.op.toString());
		sb.append("\t needExplanation:");
		sb.append(this.needExplain);
		sb.append("\t needHighlight:");
		sb.append(this.highlight);
		sb.append("\t need fill attribute:");
		sb.append(this.fillAttribute);
		sb.append("\t sort:");
		if(this.sort==null)
			sb.append("null");
		else
			sb.append(this.sort.toString());
		sb.append("\t filter:");
		if(this.filter==null)
			sb.append("null");
		else
			sb.append(this.filter.toString());
		return sb.toString();
	}
}
