package com.renren.hydra.search.scorer;

import org.apache.log4j.Logger;
import org.apache.lucene.util.PriorityQueue;

import com.renren.hydra.search.sort.FieldComparator;
import com.renren.hydra.search.sort.SortField;

public abstract class OneSortFieldScoreDocPriorityQueue<T extends IDocScoreAble> extends PriorityQueue<T> {
	private static Logger logger = Logger.getLogger(OneSortFieldScoreDocPriorityQueue.class);
	
	protected SortField sortField;
	protected FieldComparator comparator;
	protected float score1;
	protected float score2;
	protected Comparable v1;
	protected Comparable v2;
	protected int ret;
	
	public OneSortFieldScoreDocPriorityQueue(int maxSize){
		this(maxSize,null);
	}
	
	public OneSortFieldScoreDocPriorityQueue(int maxSize,SortField sortField){
		initialize(maxSize);
		this.sortField = sortField;
		this.comparator = this.sortField.getComparator();
		logger.debug("sort field : "+this.sortField.toString());
	}
	
	
	public static class SearcherScoreDocPriorityQueue<T extends IDocScoreAble> 
		extends OneSortFieldScoreDocPriorityQueue<T>{

		public SearcherScoreDocPriorityQueue(int maxSize) {
			super(maxSize);
		}
		
		public SearcherScoreDocPriorityQueue(int maxSize,SortField sortField){
			super(maxSize,sortField);
		}
		
		@Override
		protected boolean lessThan(T a, T b) {
			v1 = a.getFieldValue(0);
			v2 = b.getFieldValue(0);
			ret =0;
			if(comparator == null || v1 == null || v2 ==null){
				ret=0;
			}else{
				ret = comparator.compare(v1,v2);
			}
			if(ret!=0)
				return ret > 0;
			ret = Float.compare(a.getScore(), b.getScore());
			if(ret!=0)
				return ret < 0;
			return a.getUID() - b.getUID() < 0;
		}
	}
	
	public static class SearcherReverseScoreDocPriorityQueue<T extends IDocScoreAble> 
		extends OneSortFieldScoreDocPriorityQueue<T>{

		public SearcherReverseScoreDocPriorityQueue(int maxSize) {
			super(maxSize);
		}
		public SearcherReverseScoreDocPriorityQueue(int maxSize,SortField sortField){
			super(maxSize,sortField);
		}
		
		@Override
		protected boolean lessThan(T a, T b) {
			v1 = a.getFieldValue(0);
			v2 = b.getFieldValue(0);
			ret=0;
			if(comparator == null || v1 == null || v2 ==null){
				ret=0;
			}else{
				ret = comparator.compare(v1,v2);
			}
			if(ret!=0)
				return ret < 0;
			ret = Float.compare(a.getScore(), b.getScore());
			if(ret!=0)
				return ret < 0;
			return a.getUID() - b.getUID() < 0;
		}
		
	}
	
	public static class MergerScoreDocPriorityQueue<T extends IDocScoreAble> 
		extends OneSortFieldScoreDocPriorityQueue<T>{

		public MergerScoreDocPriorityQueue(int maxSize) {
			super(maxSize);
		}
		public MergerScoreDocPriorityQueue(int maxSize,SortField sortField){
			super(maxSize,sortField);
		}
	
		@Override
		protected boolean lessThan(T a, T b) {
			Comparable v1 = a.getFieldValue(0);
			Comparable v2 = b.getFieldValue(0);
			int c=0;
			if(comparator == null || v1 == null || v2 ==null){
				c=0;
			}else{
				c = comparator.compare(v1,v2);
			}
			if(c!=0)
				return c < 0;
			c = Float.compare(a.getScore(), b.getScore());
			if(c!=0)
				return c > 0;
			return a.getUID() - b.getUID() > 0;
		}
	
	}
	
	public static class MergerReverseScoreDocPriorityQueue<T extends IDocScoreAble> 
		extends OneSortFieldScoreDocPriorityQueue<T>{

		public MergerReverseScoreDocPriorityQueue(int maxSize) {
			super(maxSize);
		}
		public MergerReverseScoreDocPriorityQueue(int maxSize,SortField sortField){
			super(maxSize,sortField);
		}

		@Override
		protected boolean lessThan(T a, T b) {
			Comparable v1 = a.getFieldValue(0);
			Comparable v2 = b.getFieldValue(0);
			int c=0;
			if(comparator == null || v1 == null || v2 ==null){
				c=0;
			}else{
				c = comparator.compare(v1,v2);
			}
			if(c!=0)
				return c > 0;
			c = Float.compare(a.getScore(), b.getScore());
			if(c!=0)
				return c > 0;
				return a.getUID() - b.getUID() > 0;
		}

	}
}
