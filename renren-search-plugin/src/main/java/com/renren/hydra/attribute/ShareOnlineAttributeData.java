package com.renren.hydra.attribute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import com.renren.hydra.util.BitMap;

import org.apache.lucene.util.PriorityQueue;

import com.renren.hydra.attribute.OnlineAttributeData;

public class ShareOnlineAttributeData extends OnlineAttributeData {
	private static final long serialVersionUID = 1L;

	public static final String SUMMARY_FIELD = "summary";
	public static final String TITLE_FIELD = "title";

	private static final Logger logger = Logger
			.getLogger(ShareOnlineAttributeData.class);
	public static int MAX_ITEM_COUNT = 10;
	public long firstCreateTime;
	public long lastCreateTime;

	public static class Item {
		public long id;
		public int userId;
		public long createTime;

		public Item() {

		}

		public Item(long _id, int _userId, long _createTime) {
			id = _id;
			userId = _userId;
			createTime = _createTime;
		}
	}

	public class CreateTimePriorityQueue extends PriorityQueue<Item> {
		boolean isBigHead;

		public CreateTimePriorityQueue(boolean isBigHead) {
			super.initialize(MAX_ITEM_COUNT);
			this.isBigHead = isBigHead;
		}

		@Override
		protected boolean lessThan(Item a, Item b) {
			if (isBigHead) {
				return a.createTime > b.createTime;
			} else {
				return a.createTime < b.createTime;
			}
		}

		public void setBigHead() {
			isBigHead = true;
		}

		public void setSmallHead() {
			isBigHead = false;
		}
	}

	public CreateTimePriorityQueue queue;
	public Set<Long> deleteIds;

	public ShareOnlineAttributeData(boolean isBigHead) {
		queue = new CreateTimePriorityQueue(isBigHead);
		deleteIds = new HashSet<Long>();

	}

	public void addItem(long id, int userId, long createTime) {
		if (createTime > lastCreateTime) {
			lastCreateTime = createTime;
		}

		Item item = new Item(id, userId, createTime);
		queue.insertWithOverflow(item);
	}

	public void deleteId(long id, int userId) {
		deleteIds.add(id);
	}

	public Item getOldestItem(BitMap prohibitUsers, BitMap invalidUsers) {
		Item ret = null;
		while (queue.size() > 0) {
			ret = queue.top();
			long id = ret.id;
			if (deleteIds.contains(id)) {
				logger.debug("skip deleted shareid: " + id);
				queue.pop();
				continue;
			}

			int userId = ret.userId;
			if (prohibitUsers != null && prohibitUsers.get(userId)) {
				logger.debug("skip prohibit userid: " + userId);
				queue.pop();
				continue;
			} else if (invalidUsers != null && invalidUsers.get(userId)) {
				logger.debug("skip invalid userid: " + userId);
				queue.pop();
				continue;
			}
			break;
		}

		if (queue.size() == 0) {
			return null;
		} else {
			return ret;
		}
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		writeOther(out);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		readOther(in);
	}

	public void writeOther(DataOutput out) throws IOException {
		out.writeLong(firstCreateTime);
		out.writeLong(lastCreateTime);

		int pqSize = queue.size();
		out.writeInt(pqSize);

		for (int i = 0; i < pqSize; i++) {
			Item item = queue.pop();
			out.writeLong(item.id);
			out.writeInt(item.userId);
			out.writeLong(item.createTime);
		}

		int setSize = deleteIds.size();
		out.writeInt(setSize);
		for (Iterator<Long> it = deleteIds.iterator(); it.hasNext();) {
			out.writeLong((Long) it.next());
		}
	}

	public void readOther(DataInput in) throws IOException {

		firstCreateTime = in.readLong();
		lastCreateTime = in.readLong();

		int pqSize = in.readInt();
		for (int i = 0; i < pqSize; i++) {
			Item item = new Item();
			item.id = in.readLong();
			item.userId = in.readInt();
			item.createTime = in.readLong();
			queue.add(item);
		}

		int setSize = in.readInt();
		for (int i = 0; i < setSize; i++) {
			deleteIds.add(in.readLong());
		}
	}

	public void updateCreateTime(long time) {
		if (time > lastCreateTime) {
			lastCreateTime = time;
		} else if (time < firstCreateTime) {
			firstCreateTime = time;
		}
	}

	public void updateCreateTime(ShareOnlineAttributeData data) {
		if (data.lastCreateTime > lastCreateTime) {
			lastCreateTime = data.lastCreateTime;
		}

		if (data.firstCreateTime < firstCreateTime) {
			firstCreateTime = data.firstCreateTime;
		}
	}

	@Override
	public Comparable getAttributeValue(String key) {
		return null;
	}

	@Override
	public void putAttribute(String key, Comparable value) {
		
	}

	@Override
	public Map<String, Comparable> getAttributeDataMap() {
		return null;
	}

	@Override
	public Comparable getAttributeValue(int index) {
		return null;
	}


}
