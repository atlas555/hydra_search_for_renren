package com.renren.hydra.index;

import java.util.Comparator;

public class DefaultVersionComparator implements Comparator<String> {
	
	private static final int MAX_ID = 8;
	private long[] leftOffsets;
	private long[] rightOffsets;

	public DefaultVersionComparator() {
		leftOffsets = new long[MAX_ID];
		rightOffsets = new long[MAX_ID];
	}

	public int compare(String version1, String version2) {
		parseVersion(version1, leftOffsets);
		parseVersion(version2, rightOffsets);

		for (int i = 0; i < MAX_ID; i++) {
			if (leftOffsets[i] < rightOffsets[i]) {
				if (rightOffsets[i] == Long.MAX_VALUE) {
					return 1;
				} else {
					return -1;
				}
			} else if (leftOffsets[i] > rightOffsets[i]) {
				if (leftOffsets[i] == Long.MAX_VALUE) {
					return -1;
				} else {
					return 1;
				} 
			} else {
				continue;
			}
		}
		return 0;
	}

    public static void parseVersion(String version, long[] offsets) {
		for (int i = 0; i < offsets.length; i++) {
			offsets[i] = Long.MAX_VALUE;
		}

		if (version == null) {
			return;
		}

		String[] sp = version.split("_");
		for (String p : sp) {
			String[] pp = p.split(":");
			int brokerid = Integer.parseInt(pp[0]);
			long offset = Long.parseLong(pp[1]);
			offsets[brokerid] = offset;
		}
	}

	public static String toVersion(long[] offsets) {
		StringBuilder version = new StringBuilder();
		for (int i = 0; i < offsets.length; i++) {
			if (offsets[i] < Long.MAX_VALUE) {
				version.append(i).append(":").append(offsets[i]).append("_");
			}
		}

		return version.substring(0, version.length() - 1);
	}
}
