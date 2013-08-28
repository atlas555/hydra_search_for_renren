package com.renren.hydra.search.scorer;


import org.apache.log4j.Logger;
import org.apache.lucene.util.PriorityQueue;

import com.renren.hydra.search.sort.FieldComparator;
import com.renren.hydra.search.sort.SortField;

/*
 * 按域排序，默认升序排列，可以通过设置SortField中的reverse = true 变为降序
 * 
 *
 */

public class HydraSortScoreDocPriorityQueue<T extends IDocScoreAble> extends PriorityQueue<T> {
	private static Logger logger = Logger.getLogger(HydraSortScoreDocPriorityQueue.class);
	
	private int[] reverseMul;
	private FieldComparator[] comparators;
	private SortField[] sortFields;
	private int mergeMul;
	
	public HydraSortScoreDocPriorityQueue(int maxSize){
		this(maxSize,null,false);
	}
	
	public HydraSortScoreDocPriorityQueue(int maxSize,boolean reverse){
		this(maxSize,null,reverse);
	}
	
	public HydraSortScoreDocPriorityQueue(int maxSize,SortField[] sortFields){
		this(maxSize,sortFields,false);
	}

	public HydraSortScoreDocPriorityQueue(int maxSize,SortField[] sortFields,boolean merge){
		initialize(maxSize);
		this.mergeMul = merge ? -1 : 1;
		this.sortFields = sortFields;
		if(this.sortFields!=null){
			int num = this.sortFields.length;
			this.reverseMul = new int[num];
			this.comparators = new FieldComparator[num];
		
			for(int i=0;i<num;++i){
				logger.debug("sort field : "+sortFields[i].toString()+" reverse:"+(this.sortFields[i].isReverse()?"yes":"no"));
				reverseMul[i] = this.sortFields[i].isReverse() ? -1 : 1;
				comparators[i] = this.sortFields[i].getComparator();
			}
		
			logger.debug("size:"+maxSize+ " num of sortfields:"+num+" merge:" + (merge ?"yes":"no"));
		}else{
			this.comparators = null;
			this.reverseMul = null;
		}
	}
	
	protected boolean nonSortLessThan(T a, T b){
		if(a.getScore()!=b.getScore())
			return this.mergeMul * (a.getScore()-b.getScore()) <0;
		return this.mergeMul * (a.getUID() - b.getUID()) < 0; 
	}
	
	@Override
	protected boolean lessThan(T a, T b) {
		int numComparators = comparators==null ? 0 : this.comparators.length;
		for (int i = 0; i < numComparators; ++i) {
			Comparable v1 = a.getFieldValue(i);
			Comparable v2 = b.getFieldValue(i);
			if(comparators[i] == null || v1 == null || v2 ==null){
				logger.debug("comparator is "+ (comparators[i]==null?"null":"not null") + 
						" field value of a is "+v1+" field value of b is "+v2);
				continue;
			}
			int c = this.mergeMul * reverseMul[i] * comparators[i].compare(v1,v2);
	        if (c != 0) {
	          return c > 0;
	        }
	    }
		return nonSortLessThan(a,b);
	}

}
