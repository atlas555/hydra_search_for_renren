package com.renren.hydra.search.scorer;

import org.apache.lucene.util.PriorityQueue;

public abstract class HydraScoreDocPriorityQueue  <T extends IDocScoreAble> extends PriorityQueue<T>{

	public HydraScoreDocPriorityQueue(int maxSize){
		initialize(maxSize);
	}
	
	protected float score1;
	protected float score2;
	
	public static class SearcherScoreDocPriorityQueue<T extends IDocScoreAble>  extends HydraScoreDocPriorityQueue<T>{
		public SearcherScoreDocPriorityQueue(int maxSize) {
			super(maxSize);
		}
		
		@Override
		protected boolean lessThan(T a, T b) {
			score1 = a.getScore();
			score2 = b.getScore();
			if(score1==score2)
				return a.getUID() - b.getUID() < 0;
			return score1 - score2 < 0;
		}
	}
	
	public static class MergerScoreDocPriorityQueue <T extends IDocScoreAble> extends HydraScoreDocPriorityQueue<T>{
		public MergerScoreDocPriorityQueue(int maxSize) {
			super(maxSize);
		}

		@Override
		protected boolean lessThan(T a, T b) {
			int c = Float.compare(a.getScore(), b.getScore());
			if(c!=0)
				 return c > 0;
			return a.getUID() - b.getUID() > 0;
		}
	}
	
	

}
