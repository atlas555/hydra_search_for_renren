package com.renren.hydra.util;

import java.util.Arrays;

public class BitMap
{
	private byte[] buf;

	public BitMap(int num) {
		int n = num >> 3;
		if ((num & 7) == 0) {
			buf = new byte[n];
		} else {
			buf = new byte[n + 1];
		}
	}
	
	public boolean get(int i) {
		int pos = i >> 3;
		int off = i & 7;
		
		byte b = (byte)(1 << off);
		byte r = (byte)(buf[pos] & b);

		return r != 0;
	}

	public void set(int i, boolean b) {
		int pos = i >> 3;
		int off = i & 7;
		
		byte c = (byte)(1 << off);
		if(b) {
			buf[pos] |= c;
		} else {
			byte d = (byte)(~c);
			buf[pos] &= d;
		}
	}
	
	public void clear() {
		Arrays.fill(buf, (byte)0);
	}
}
