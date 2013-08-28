package com.renren.hydra.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class HydraLong implements Writable {
	private long value;

	public long get() {
		return value;
	}

	public void set(long value) {
		this.value = value;
	}
	
	public HydraLong(){
		this(0L);
	}
	
	public HydraLong(long v){
		this.value = v;
	}
	
	public int hashCode() {
		 return (int)(value ^ (value >>> 32));
	}
	
    public boolean equals(Object obj) {
        if (obj instanceof HydraLong) {
            return value == ((HydraLong)obj).get();
        }
        return false;
    }
    
    public String toString(){
    	return Long.toString(value);
    }
    
    public int compareTo(HydraLong anotherLong) {
    	 long x  = this.value;
    	 long y = anotherLong.value;
    	 return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.value);
	}
}
