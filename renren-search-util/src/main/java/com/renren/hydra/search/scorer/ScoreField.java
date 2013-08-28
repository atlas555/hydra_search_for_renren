package com.renren.hydra.search.scorer;

/*
 * 保存各个域的打分信息
 * fieldName: 域名
 * weight: 该域的权重
 */
public  class ScoreField{
	private String fieldName;
    private float weight;
    public ScoreField(String _fieldName, float _weight){
		this.fieldName = _fieldName;
			this.weight = _weight;
	}
           
    public String getFieldName(){
		return this.fieldName;
	}
          
	public float getWeight(){
		return this.weight;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("fieldname:");
		sb.append(this.fieldName);
		sb.append("\tweight:");
		sb.append(this.weight);
		return sb.toString();
	}
}
