package com.renren.hadoop.index.module;

import org.apache.lucene.index.Payload;

public abstract class AbstractEncoder implements PayloadEncoder{
        public Payload encode(char[] buffer) {
                return encode(buffer, 0, buffer.length);
        }
}

