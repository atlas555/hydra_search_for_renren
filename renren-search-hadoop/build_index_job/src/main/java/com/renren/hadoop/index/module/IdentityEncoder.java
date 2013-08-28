package com.renren.hadoop.index.module;

import org.apache.lucene.index.Payload;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.io.UnsupportedEncodingException;

public class IdentityEncoder extends AbstractEncoder implements PayloadEncoder{

        protected Charset charset = Charset.forName("UTF-8");
        protected String charsetName = "UTF-8";

        public IdentityEncoder() {
        }

        public IdentityEncoder(Charset charset) {
                this.charset = charset;
                charsetName = charset.name();
        }


        public Payload encode(char[] buffer, int offset, int length) {
                String tmp = new String(buffer, offset, length);
                Payload result = null;
                try {
                        result = new Payload(tmp.getBytes(charsetName));
                } catch (UnsupportedEncodingException e) {
                }

                return result;
        }
}

