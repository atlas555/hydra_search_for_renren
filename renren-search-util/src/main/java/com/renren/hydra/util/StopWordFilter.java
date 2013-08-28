package com.renren.hydra.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StopWordFilter {
	public static final String[] stopWordList = new String[]{"的","国","人","一","中","学",
		"在","大","有","年","了","是","和","不","为","上","会","家","生","业","地","出","个",
		"工","这","以","成","发","作","我"
		};
	
	public static Set<String> stopWordsSet = new HashSet<String>(Arrays.asList(stopWordList));
	
	public static boolean filter(String word){
		return stopWordsSet.contains(word);
	}
	
	public  static void main(String[] args){
		System.out.println(StopWordFilter.filter("的"));
		System.out.println(StopWordFilter.filter("你"));
		System.out.println(StopWordFilter.filter(""));
		System.out.println(StopWordFilter.filter(null));
		System.out.println(StopWordFilter.filter("我"));
	}
	
}
