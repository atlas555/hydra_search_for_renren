package com.renren.hydra.search.scorer;

public interface IDocScoreAble {
	public float getScore();
	public long getUID();
	public Comparable getFieldValue(int index);
}
