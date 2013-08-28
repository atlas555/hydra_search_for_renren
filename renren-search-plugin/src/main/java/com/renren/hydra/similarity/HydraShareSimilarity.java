package com.renren.hydra.similarity;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.Math;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.attribute.ShareOfflineAttributeData;

import com.renren.hydra.config.schema.Schema;

import com.renren.hydra.search.scorer.TermMatchInfo;
import com.renren.hydra.search.similarity.HydraSimilarity;
import com.renren.hydra.attribute.ShareOnlineAttributeData;

public class HydraShareSimilarity extends HydraSimilarity {
	private static Logger logger = Logger.getLogger(HydraShareSimilarity.class);

	private static final String UID = "uid";
	private static final String DOC_ID = "doc_id";
	private static final String TITLE_TTF = "title_ttf";
	private static final String SUMMARY_TTF = "summary_ttf";
	private static final String MATCH_TERM_COUNT = "match_term_count";
	private static final String MATCH_TERM_OFFSET = "match_term_offset";
	private static final String MATCH_TERM_TF = "match_term_tf";
	private static final String MATCH_TERM_IDF = "match_term_idf";
	private static final String SCORE = "score";
	private static final String QUALITY = "quality";
	private static final String TEXT_SCORE = "text_score";
	private static final String DELTA_TIME = "delta_time";
	private static final String LAST_UPTIME = "last_uptime";
	private static final String OLD_QUALITY = "old_quality";
	private static final String HEAT_SCORE = "heat_score";
	private static final String IS_AD = "is_ad";
	private static final String FINGER_PRINT = "finger_print";

	private static final double DEFAULT_QUALITY = 0.05;
	private static final float TITLE_WEIGHT = 0.62f;
	private static final float SUMMARY_WEIGHT = 0.38f;
	private static final float TEXT_SCORE_NORM = 20.0f;
	private static final float TEXT_SCORE_WEIGHT = 0.4f;
	private static final float QUALITY_WEIGHT = 0.6f;

	private long curUid;
	private int curDocId;
	private int curTitleTotalTf;
	private int curSummaryTotalTf;
	private int curMatchTermCnt;
	private int[] curMatchTermOffsets;
	private int[] curMatchTermTfs;
	private float[] curMatchTermIdfs;
	private double curQuality;
	private float curScore;
	private float curTextScore;

	private Term[] terms;
	private int termCntInQuery;

	private long curDeltaTime;
	private long curLastUpTime;
	private double curOldQuality;
	private double curHeatScore;
	private boolean curIsAd;
	private long curFingerPrint;

	public HydraShareSimilarity(
			Schema schema,
			Query query,
			ConcurrentHashMap<Long, OnlineAttributeData> attributeDataTable,
			ConcurrentHashMap<Long, OfflineAttributeData> offlineAttributeDataTable,
			boolean showExplain) {
		super(schema, query, showExplain);

		if (showExplain) {
			curUid = -1;
			curDocId = -1;
			curTitleTotalTf = -1;
			curSummaryTotalTf = -1;
			Set<Term> termSet = new HashSet<Term>();
			query.extractTerms(termSet);
			termCntInQuery = termSet.size();
			terms = new Term[termCntInQuery];
			int offset = 0;
			for (Iterator it = termSet.iterator(); it.hasNext();) {
				Term term = (Term) it.next();
				terms[offset] = term;
				offset++;
			}

			curMatchTermCnt = -1;
			curMatchTermOffsets = null;
			curMatchTermTfs = null;
			curMatchTermIdfs = null;
			curScore = 0.0f;
			curQuality = DEFAULT_QUALITY;
			curTextScore = 0.0f;
			curDeltaTime = 0;
			curLastUpTime = 0;
			curOldQuality = 0.0f;
			curHeatScore = 1.0f;
			curIsAd = false;
			curFingerPrint = 0;
		}
	}

	@Override
	public float score(int docid, int baseDocId, long uid,
			TermMatchInfo[] matchInfos, int matchTermCount,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) throws Exception {

		curScore = 0.0f;
		curTextScore = 0.0f;
		curHeatScore = 1.0f;
		curQuality = DEFAULT_QUALITY;
		curOldQuality = 0.0f;
		curIsAd = false;
		curFingerPrint = 0;
		float titleRet = 0.0f;
		float summaryRet = 0.0f;
		for (int i = 0; i < matchTermCount; i++) {
			TermMatchInfo info = matchInfos[i];
			Term term = info.term;
			if (term.field().equals("title")) {
				titleRet += info.idf * Math.sqrt(info.tf);
			} else if (term.field().equals("summary")) {
				summaryRet += info.idf * Math.sqrt(info.tf);
			} else {
				logger.error("no supported field, ignore.");
				continue;
			}
		}

		ShareOnlineAttributeData attrData = (ShareOnlineAttributeData)onlineAttributeData;
		if (attrData == null) {
			throw new Exception("cannot get attribute data for uid: " + uid);
		}

		if (attrData.getTTF(_schema.getIndexIdByFieldName(attrData.SUMMARY_FIELD)) != 0) {
			curTextScore += summaryRet
					* SUMMARY_WEIGHT
					/ (float) Math
							.sqrt(attrData.getTTF(_schema.getIndexIdByFieldName(attrData.SUMMARY_FIELD)));
		}
		if (attrData.getTTF(_schema.getIndexIdByFieldName(attrData.TITLE_FIELD)) != 0) {
			curTextScore += titleRet * TITLE_WEIGHT
					/ (float) Math.sqrt(attrData.getTTF(_schema.getIndexIdByFieldName(attrData.TITLE_FIELD)));
		}

		ShareOfflineAttributeData offAttrData = (ShareOfflineAttributeData) offlineAttributeData;
		if (offAttrData == null) {
			curQuality = DEFAULT_QUALITY;
		} else {
			long firstCreateTime = attrData.firstCreateTime;
			long lastCreateTime = attrData.lastCreateTime;
			Date dt = new Date();
			long curTime = dt.getTime();
			curDeltaTime = (curTime - firstCreateTime) / 3600000;
			curLastUpTime = (curTime - lastCreateTime) / 3600000;
			curOldQuality = offAttrData.qualityScore;
			curHeatScore = offAttrData.heatScore + 1.0;
			curFingerPrint = offAttrData.contentFingerPrint;
			float fqparam = 1.0f;
			if (curLastUpTime >= 0 && curLastUpTime < 24 * 10) {
				fqparam = (float) Math.log10(24 * 10 - curLastUpTime + 10);
			}
			if (fqparam < curHeatScore) {
				fqparam = (float) curHeatScore;
			}
			curQuality = curOldQuality * (float) Math.log10(curDeltaTime + 10)
					* 2.5 * fqparam / (float) Math.log(curDeltaTime + Math.E);
			curIsAd = offAttrData.isAd;

			if (curIsAd == true) {
				curQuality = curQuality / 10.0;
			}
			curHeatScore = fqparam;
		}

		curScore = curTextScore * TEXT_SCORE_WEIGHT / TEXT_SCORE_NORM
				+ QUALITY_WEIGHT * (float) curQuality;

		if (showExplain) {
			curUid = uid;
			curDocId = docid + baseDocId;
			curTitleTotalTf = attrData.getTTF(_schema.getIndexIdByFieldName(attrData.TITLE_FIELD));
			curSummaryTotalTf = attrData.getTTF(_schema.getIndexIdByFieldName(attrData.SUMMARY_FIELD));
			curMatchTermCnt = matchTermCount;
			curMatchTermOffsets = new int[matchTermCount];
			curMatchTermTfs = new int[matchTermCount];
			curMatchTermIdfs = new float[matchTermCount];

			for (int i = 0; i < termCntInQuery; i++) {
				TermMatchInfo info = matchInfos[i];
				Term term = info.term;
				int offset = 0;
				for (; offset < terms.length; offset++) {
					if (term.equals(terms[offset])) {
						curMatchTermOffsets[i] = offset;
						break;
					}
				}

				if (offset == terms.length) {
					throw new Exception("cannot find term: " + term
							+ " in query");
				}
				curMatchTermTfs[i] = info.tf;
				curMatchTermIdfs[i] = info.idf;
			}
		}
		HydraShareScoreDoc tempScoreDoc = null;
		HydraShareScoreDoc scoreDoc = null;
		if (tempScoreDoc == null)
			scoreDoc = new HydraShareScoreDoc(docid + baseDocId, curScore);
		else
			scoreDoc = (HydraShareScoreDoc) tempScoreDoc;

		scoreDoc.doc = docid + baseDocId;
		scoreDoc.score = curScore;
		scoreDoc.firstCreateTime = attrData.firstCreateTime;
		scoreDoc.lastCreateTime = attrData.lastCreateTime;
		if (offAttrData != null) {
			scoreDoc.viewCount = offAttrData.viewCount;
			scoreDoc.shareCount = offAttrData.shareCount;
		}
		scoreDoc.explainationStr = this.explain(getKVForExplain());
		scoreDoc._partition = partition;
		scoreDoc._uid = uid;
		return curScore;
	}

	public Map<String, Object> getKVForExplain() {
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put(UID, curUid);
		ret.put(DOC_ID, curDocId);
		ret.put(TITLE_TTF, curTitleTotalTf);
		ret.put(SUMMARY_TTF, curSummaryTotalTf);
		ret.put(MATCH_TERM_COUNT, curMatchTermCnt);
		ret.put(MATCH_TERM_OFFSET, curMatchTermOffsets);
		ret.put(MATCH_TERM_TF, curMatchTermTfs);
		ret.put(MATCH_TERM_IDF, curMatchTermIdfs);
		ret.put(SCORE, curScore);
		ret.put(QUALITY, curQuality);
		ret.put(TEXT_SCORE, curTextScore);
		ret.put(DELTA_TIME, curDeltaTime);
		ret.put(LAST_UPTIME, curLastUpTime);
		ret.put(OLD_QUALITY, curOldQuality);
		ret.put(HEAT_SCORE, curHeatScore);
		ret.put(IS_AD, curIsAd);
		ret.put(FINGER_PRINT, curFingerPrint);

		return ret;
	}

	public String explain(Map<String, Object> kvForExplain) {
		long uid = (Long) kvForExplain.get(UID);
		int docid = (Integer) kvForExplain.get(DOC_ID);
		int titleTtf = (Integer) kvForExplain.get(TITLE_TTF);
		int summaryTtf = (Integer) kvForExplain.get(SUMMARY_TTF);
		int matchTermCout = (Integer) kvForExplain.get(MATCH_TERM_COUNT);
		int[] matchTermOffsets = (int[]) kvForExplain.get(MATCH_TERM_OFFSET);
		int[] matchTermTfs = (int[]) kvForExplain.get(MATCH_TERM_TF);
		float[] matchIdfs = (float[]) kvForExplain.get(MATCH_TERM_IDF);
		float score = (Float) kvForExplain.get(SCORE);
		double quality = (Double) kvForExplain.get(QUALITY);
		double oldQuality = (Double) kvForExplain.get(OLD_QUALITY);
		float textScore = (Float) kvForExplain.get(TEXT_SCORE) / 20.0f;
		long tmpDeltaTime = (Long) kvForExplain.get(DELTA_TIME);
		long tmpLastUpTime = (Long) kvForExplain.get(LAST_UPTIME);
		double heatScore = (Double) kvForExplain.get(HEAT_SCORE);
		boolean bIsAd = (Boolean) kvForExplain.get(IS_AD);
		long tmpFingerPrint = (Long) kvForExplain.get(FINGER_PRINT);

		StringBuilder sb = new StringBuilder();
		StringBuilder explanation = new StringBuilder();
		explanation.append("Explanation: ");
		sb.append("Match info for score of docid ");
		sb.append(Integer.toString(docid));
		sb.append("; uid: ");
		sb.append(Long.toString(uid));
		sb.append("; paritition: ");
		sb.append(Integer.toString(partition));
		sb.append("; total tf of title field: ");
		sb.append(Integer.toString(titleTtf));
		sb.append("; total tf of summary field: ");
		sb.append(Integer.toString(summaryTtf));
		sb.append("; matched term count: ");
		sb.append(Integer.toString(matchTermCout));
		sb.append("; matched terms: ");
		explanation.append("(");
		for (int i = 0; i < matchTermCout; i++) {
			sb.append("{");
			Term term = terms[matchTermOffsets[i]];
			sb.append("field: ");
			sb.append(term.field());
			sb.append(", value: ");
			sb.append(term.text());
			sb.append(", tf: ");
			sb.append(Integer.toString(matchTermTfs[i]));
			sb.append(", idf: ");
			sb.append(Float.toString(matchIdfs[i]));
			sb.append("}");

			if (term.field().equals("title")) {
				explanation.append(Integer.toString(matchTermTfs[i]))
						.append(" * ").append(Float.toString(matchIdfs[i]))
						.append(" * ").append(Float.toString(TITLE_WEIGHT))
						.append(" / ").append(Integer.toString(titleTtf));
			} else if (term.field().equals("summary")) {
				explanation.append(Integer.toString(matchTermTfs[i]))
						.append(" * ").append(Float.toString(matchIdfs[i]))
						.append(" * ").append(Float.toString(SUMMARY_WEIGHT))
						.append(" / ").append(Integer.toString(summaryTtf));
			}

			if (i < matchTermCout - 1) {
				explanation.append(" + ");
			}
		}
		explanation.append(") * ");
		explanation.append(Float.toString(TEXT_SCORE_WEIGHT));
		explanation.append(" / ");
		explanation.append(Float.toString(TEXT_SCORE_NORM));

		sb.append("; text score: ").append(Float.toString(textScore));
		sb.append("; doc quality: ").append(Double.toString(quality));
		sb.append("; old quality: ").append(Double.toString(oldQuality));
		sb.append("; heat score: ").append(Double.toString(curHeatScore));
		sb.append("; is ad: ").append(Boolean.toString(bIsAd));
		sb.append("; delta time: ").append(Long.toString(tmpDeltaTime));
		sb.append("; last update time: ").append(Long.toString(tmpLastUpTime));
		sb.append("; finger print: ").append(Long.toString(tmpFingerPrint));
		sb.append("; final score: ").append(Float.toString(score));
		;
		explanation.append(" + (");
		explanation.append(Double.toString(quality));
		explanation.append(") * ");
		explanation.append(Float.toString(QUALITY_WEIGHT));
		explanation.append(" = ");
		explanation.append(Float.toString(score));

		sb.append(", ");
		return sb.append(explanation.toString()).toString();
	}
}
