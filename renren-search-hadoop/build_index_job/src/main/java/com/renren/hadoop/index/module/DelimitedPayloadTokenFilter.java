package com.renren.hadoop.index.module;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;



public final class DelimitedPayloadTokenFilter extends TokenFilter {
	public static final char DEFAULT_DELIMITER = '|';
	protected char delimiter = DEFAULT_DELIMITER;
	protected TermAttribute termAtt;
	protected PayloadAttribute payAtt;
	protected PayloadEncoder encoder;

	protected DelimitedPayloadTokenFilter(TokenStream input) {
		this(input, DEFAULT_DELIMITER, new IdentityEncoder());
	}

	public DelimitedPayloadTokenFilter(TokenStream input, char delimiter, 
			PayloadEncoder encoder) 
	{
		super(input);
		termAtt = addAttribute(TermAttribute.class);
		payAtt = addAttribute(PayloadAttribute.class);
		this.delimiter = delimiter;
		this.encoder = encoder;
	}

	@Override
	public boolean incrementToken() throws IOException {
		boolean result = false;
		if (input.incrementToken()) {
			final char[] buffer = termAtt.termBuffer();
			final int length = termAtt.termLength();
      
			boolean seen = false;
			for (int i = 0; i < length; i++) {
				if (buffer[i] == delimiter) {
					termAtt.setTermBuffer(buffer, 0, i);
					payAtt.setPayload(encoder.encode(buffer, i + 1, 
								(length - (i + 1))));
					seen = true;
					break;
				}
			}
			
			if (seen == false) {
				payAtt.setPayload(null);
			}
			result = true;
		}
		return result;
	}
}

