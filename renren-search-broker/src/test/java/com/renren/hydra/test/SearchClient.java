package com.renren.hydra.test;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.lucene.queryParser.QueryParser;
import org.json.JSONObject;

import com.renren.hydra.client.adapter.HydraAdapter;
import com.renren.hydra.client.Condition;
import com.renren.hydra.client.FieldQueryCondition;
import com.renren.hydra.client.SearchType;
import com.renren.hydra.search.HydraResult;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.sort.Sort; 
import com.renren.hydra.search.sort.SortField;
import com.renren.hydra.search.sort.SortField.DataSourceType;
import com.renren.hydra.search.sort.SortField.DataType;
import com.renren.hydra.thirdparty.zkmanager2.ZkManager;
import com.renren.searchrelation.adapter.RelationAdapter;
import com.renren.searchrelation.util.RedisType;


public class SearchClient {
	public static void main(String[] args) throws InterruptedException {
		Condition condition = new Condition();
		String business = "status";
		condition.setQuery("你好");
		//condition.setHighlight(false);
		//Sort sort = new Sort(new SortField[]{new SortField("dtime", true)}, false);
		//condition.setSort(sort);
		
		int userId = 1845993119;
		condition.setUserId(userId);
		//condition.setHighlight(false);
		
		//condition.addGreaterThanAttributeFilter("dtime", "2013-01-17 00:00:00");
		//condition.addLessThanAttributeFilter("dtime", "2013-01-19 00:00:00");
		//Map<Integer, byte[]> allfriendsInfo = RelationAdapter.getRelationV3(userId, 3, business,RedisType.BEIJING);
		//System.out.println(allfriendsInfo.size());
		//Map<MutableInt,Short> friends1 = allfriendsInfo.get(1);
		//condition.addEqualAttributeFilter("isRepeat", "1");
		
		//Map<Integer,Map<Integer,Short>> friendInfos = RelationAdapter.getRelationV2(userId, 3, business,RedisType.BEIJING);
		//condition.setFriendsInfo(friendInfos);
		//condition.setFriendsInfoBytes(allfriendsInfo);
		//condition.setSearchType(SearchType.OnlyFriends);
			
		//condition.addFieldQuery("tags", "US OR 2012");
		//condition.setOperator(QueryParser.OR_OPERATOR);
		Sort sort = new Sort(new SortField[]{new SortField("dtime",true)},false);
		//System.out.println(sort.toString());
		condition.setSort(sort);
		//condition.setNeedExplain();
		//String zkAddress="10.11.18.120:2181";
		String zkAddress="10.11.18.120:2181";
			try{
				HydraAdapter adapter = HydraAdapter.getInstance(business);
				adapter.init(zkAddress);
				Date d = new Date();
				long start = System.currentTimeMillis();
				HydraResult result = null;
				for(int i=0;i<1000;i++){
					result = adapter.search(
							condition, 0, 10);
					Thread.sleep(500);
					System.out.println(result.getTotalDocs());
				}
								
		System.out.println("total docs: " + result.getTotalDocs());
		System.out.println("number hit: " + result.getNumHits());

		Map<Long, JSONObject> content = null;
		try {
			content = result.getContent();
		} catch (Exception e) {
			System.out.println("cannot get summary");
		}

		HydraScoreDoc[] hits = result.getHits();
		for (int i = 0; i < result.getNumHits(); i++) {
			
			long uid = hits[i].getUID();
			StringBuilder sb = new StringBuilder();
			sb.append("partition: " + hits[i].getPartition() + ", docid: "
					+ hits[i].getDocId() + ", score: " + hits[i].getScore()
					+ ", uid: " + uid + ", content: ");
			JSONObject obj = null;
			try {
				obj = content.get(uid);
				sb.append(obj);
			} catch (Exception e) {
				sb.append("cannot get summary.");
			}
			if(hits[i] instanceof HydraScoreDoc){
				HydraScoreDoc hit = (HydraScoreDoc)hits[i];
				sb.append(", quantity: "+hit.getOnlineAttribute("isRepeat")+" : "+hit.getOnlineAttribute("createTime"));
			}
			System.out.println(sb.toString());
		}
		//HydraResult result2= adapter.search(
				//condition, 0, 10);
		//System.out.println(result2.getTotalDocs());
				long end = System.currentTimeMillis();
				System.out.println("time: "+(end-start));
				
				
			Thread.sleep(30000);
		}catch(Exception e){
			e.printStackTrace();
		}
			
	}
}
