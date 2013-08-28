package com.renren.hadoop.index.module;

import org.apache.log4j.Logger;

import org.apache.lucene.index.Payload;
import org.apache.lucene.util.ArrayUtil;

public class LongEncoder extends AbstractEncoder implements PayloadEncoder {
        private static Logger logger = Logger.getLogger(LongEncoder.class);

        public Payload encode(char[] buffer, int offset, int length) {
                Payload result = new Payload();
                long payload = 0;
                for (int i = 0; i < length; i++) {
                        payload = payload * 10 + (buffer[i + offset] - '0');
                }
                byte[] bytes = encodeLong(payload);
                result.setData(bytes);
                return result;
        }

        private static byte[] encodeLong(long payload){
                byte[] data = new byte[8];    
                data[0] = (byte)payload;
                data[1] = (byte)(payload >> 8);
                data[2] = (byte)(payload >> 16);
                data[3] = (byte)(payload >> 24);
                data[4] = (byte)(payload >> 32);
                data[5] = (byte)(payload >> 40);
                data[6] = (byte)(payload >> 48);
                data[7] = (byte)(payload >> 56);

                return data;
        }
}

