package com.renren.hydra.util;
public class TimeCost {
	long old;

	public TimeCost() {
		old = System.nanoTime();
	}

	public float get() {
		long res = System.nanoTime() - old;
		return (float) res / 1000000;
	}
	
	public float getReset() {
		long tmp = System.nanoTime();
		long res = tmp - old;
		old = tmp;
		return (float) res / 1000000;
	}

	public static void main(String[] args) throws InterruptedException {
		TimeCost tr = new TimeCost();
		System.out.println("cost:" + tr.get());
	}

}