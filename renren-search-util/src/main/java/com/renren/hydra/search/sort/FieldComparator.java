package com.renren.hydra.search.sort;

import java.util.Date;

public abstract class FieldComparator<T extends Comparable> {
	public abstract int compare(T v1, T v2);

	public static class StringComparator extends FieldComparator<String> {
		@Override
		public int compare(String v1, String v2) {
			return v1.compareTo(v2);
		}
	}

	public static class FloatComparator extends FieldComparator<Float> {
		@Override
		public int compare(Float v1, Float v2) {
			return v1.compareTo(v2);
		}
	}

	public static class IntComparator extends FieldComparator<Integer> {
		@Override
		public int compare(Integer v1, Integer v2) {
			return v1.compareTo(v2);
		}
	}

	public static class DoubleComparator extends FieldComparator<Double> {
		@Override
		public int compare(Double v1, Double v2) {
			return v1.compareTo(v2);
		}
	}

	public static class LongComparator extends FieldComparator<Long> {
		@Override
		public int compare(Long v1, Long v2) {
			return v1.compareTo(v2);
		}
	}

	public static class DateComparator extends FieldComparator<Date> {
		@Override
		public int compare(Date v1, Date v2) {
			return v1.compareTo(v2);
		}

	}

}
