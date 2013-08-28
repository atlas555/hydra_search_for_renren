package com.renren.hadoop.index.module;

import org.apache.lucene.index.Payload;

public interface PayloadEncoder {
        Payload encode(char[] buffer);
        Payload encode(char [] buffer, int offset, int length);
}

