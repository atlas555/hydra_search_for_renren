import java.util.Map;

import org.json.JSONObject;

import com.renren.hydra.client.adapter.HydraAdapter;
import com.renren.hydra.client.Condition;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.HydraResult;

public class SearchClient {
	public static void main(String[] args) {
		Condition condition = new Condition();
		condition.setQuery(args[0]);
		//if (args[1].equals("createTime")) {
		//	condition.addSort(args[1]);
		//} else if (args[1].equals("shareCount")) {
		//	condition.addSort(args[1]);
		//} 

                String business = args[1];
		condition.setNeedExplain();

                HydraAdapter adapter = HydraAdapter.getInstance(business);
                adapter.init(args[2]);
		HydraResult result = adapter.search(condition, 0, 20);
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
			System.out.println(sb.toString());
		}
	}
}
