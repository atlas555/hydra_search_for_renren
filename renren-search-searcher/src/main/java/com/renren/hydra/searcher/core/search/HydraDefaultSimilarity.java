package com.renren.hydra.searcher.core.search;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.lang.Math;

import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.scorer.ScoreField;
import com.renren.hydra.search.scorer.TermMatchInfo;
import com.renren.hydra.search.similarity.HydraSimilarity;

/*
 * 默认Similarity
 * 根据SearchSchema 配置的各个域进行打分，只计算文本相似度分数
 */
public class HydraDefaultSimilarity extends HydraSimilarity {

	private static Logger logger = Logger
			.getLogger(HydraDefaultSimilarity.class);
	private static final String UID = "uid";
	private static final String DOC_ID = "doc_id";

	private static final String MATCH_TERM_COUNT = "match_term_count";
	private static final String MATCH_TERM_OFFSET = "match_term_offset";
	private static final String MATCH_TERM_TF = "match_term_tf";
	private static final String MATCH_TERM_IDF = "match_term_idf";
	private static final String SCORE = "score";
	private static final String TEXT_SCORE = "text_score";
	private static final String FIELD_TEXT_SCORES = "field_text_scores";

	private long curUid;
	private int curDocId;
	private int curMatchTermCnt;
	private int[] curMatchTermOffsets;
	private int[] curMatchTermTfs;
	private float[] curMatchTermIdfs;
	private float curScore;
	private float curTextScore;

	private Term[] terms;
	private int termCntInQuery;

	private float[] fieldTextScores;

	private int numScoreFields;
	private ScoreField[] scoreFields;
	private Map<String, Integer> scoreFieldName2Id;
	private float[] scoreFieldWeight;
	private int[] scoreField2IndexId;

	public HydraDefaultSimilarity(Schema schema, Query query,
			boolean showExplain) {
		super(schema, query, showExplain);
		
		numScoreFields = _schema.getNumScoreField();
		scoreFields = _schema.getScoreFields();
		
		this.fieldTextScores = new float[numScoreFields];
		scoreFieldWeight = new float[numScoreFields];
		
		for (int i = 0; i < numScoreFields; i++) {
			scoreFieldWeight[i] = scoreFields[i].getWeight();
		}

		scoreFieldName2Id = new HashMap<String, Integer>(numScoreFields * 2);
		scoreField2IndexId = new int[numScoreFields];
		
		String[] fieldNames = _schema.getScoreFiledNames();
		String fieldName;
		Integer index;
		for (int i = 0; i < numScoreFields; i++) {
			fieldName = fieldNames[i];
			scoreFieldName2Id.put(fieldName,
					_schema.getScoreFieldIdByName(fieldName));

			index = _schema.getIndexIdByFieldName(fieldName);
			if (index != null) {
				scoreField2IndexId[i] = index;
			} else {
				scoreField2IndexId[i] = -1;
			}
		}

		if (showExplain) {
			curUid = -1;
			curDocId = -1;
			Set<Term> termSet = new HashSet<Term>();
			query.extractTerms(termSet);
			termCntInQuery = termSet.size();
			terms = new Term[termCntInQuery];
			int offset = 0;
			for (Iterator<Term> it = termSet.iterator(); it.hasNext();) {
				Term term = it.next();
				terms[offset] = term;
				offset++;
			}

			curMatchTermCnt = -1;
			curMatchTermOffsets = null;
			curMatchTermTfs = null;
			curMatchTermIdfs = null;
			curScore = 0.0f;
			curTextScore = 0.0f;
		}
	}

	public Map<String, Object> getKVForExplain() {
		if (!this.showExplain)
			return null;
		Map<String, Object> ret = new HashMap<String, Object>();
		ret.put(UID, curUid);
		ret.put(DOC_ID, curDocId);
		ret.put(MATCH_TERM_COUNT, curMatchTermCnt);
		ret.put(MATCH_TERM_OFFSET, curMatchTermOffsets);
		ret.put(MATCH_TERM_TF, curMatchTermTfs);
		ret.put(MATCH_TERM_IDF, curMatchTermIdfs);
		ret.put(SCORE, curScore);
		ret.put(TEXT_SCORE, curTextScore);
		ret.put(FIELD_TEXT_SCORES, this.fieldTextScores);

		return ret;
	}

	public String explain(Map<String, Object> kvForExplain) {
		if (kvForExplain == null)
			return "";
		long uid = (Long) kvForExplain.get(UID);
		int docid = (Integer) kvForExplain.get(DOC_ID);
		int matchTermCout = (Integer) kvForExplain.get(MATCH_TERM_COUNT);
		int[] matchTermOffsets = (int[]) kvForExplain.get(MATCH_TERM_OFFSET);
		int[] matchTermTfs = (int[]) kvForExplain.get(MATCH_TERM_TF);
		float[] matchIdfs = (float[]) kvForExplain.get(MATCH_TERM_IDF);
		float score = (Float) kvForExplain.get(SCORE);
		float textScore = (Float) kvForExplain.get(TEXT_SCORE);
		float[] fieldTextScores = (float[]) kvForExplain.get(FIELD_TEXT_SCORES);

		StringBuilder sb = new StringBuilder();
		StringBuilder explanation = new StringBuilder();
		explanation.append(" Explanation: ");
		sb.append("Match info for score of docid ");
		sb.append(Integer.toString(docid));
		sb.append("; uid: ");
		sb.append(Long.toString(uid));
		sb.append("; paritition: ");
		sb.append(Integer.toString(partition));
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

			int id = _schema.getScoreFieldIdByName(term.field());
			if (0 <= id && id < numScoreFields) {
				explanation.append(Integer.toString(matchTermTfs[i]))
						.append(" * ").append(Float.toString(matchIdfs[i]))
						.append(" * ").append(scoreFields[id].getFieldName())
						.append(":")
						.append(Float.toString(scoreFields[id].getWeight()));
			}

			if (i < matchTermCout - 1) {
				explanation.append(" + ");
			}
		}

		explanation.append(")");

		sb.append("; text score: ").append(Float.toString(textScore));
		sb.append("; final score: ").append(Float.toString(score));

		return sb.append(explanation.toString()).toString();
	}

	@Override
	public float score(int docid, int baseDocId, long uid,
			TermMatchInfo[] matchInfos, int matchTermCount,
			OnlineAttributeData onlineAttributeData,
			OfflineAttributeData offlineAttributeData) throws Exception {
		curScore = 0.0f;
		curTextScore = 0.0f;

		for (int i = 0; i < numScoreFields; ++i) {
			fieldTextScores[i] = 0.0f;
		}
		TermMatchInfo info;
		Term term;
		String field;
		Integer id;
		for (int i = 0; i < matchTermCount; ++i) {
			info = matchInfos[i];
			term = info.term;
			field = term.field();
			id = scoreFieldName2Id.get(field);
			if (id != null && 0 <= id && id < numScoreFields) {
				fieldTextScores[id] += info.idf * Math.sqrt(info.tf);
			} else {
				logger.error("no supported field:[fieldname]- " + field
						+ " ignore.");
				continue;
			}
		}

		//DefaultOnlineAttributeData attrData = (DefaultOnlineAttributeData) onlineAttributeData;
		if (onlineAttributeData == null) {
			throw new Exception("cannot get attribute data for uid: " + uid);
		}

		for (int i = 0; i < numScoreFields; ++i) {
			int ttf = onlineAttributeData.getTTF(scoreField2IndexId[i]);
			if (fieldTextScores[i] != 0.0f && ttf != 0)
				curTextScore += fieldTextScores[i] * (scoreFieldWeight[i])
						/ ((float) Math.sqrt(ttf));
		}

		curScore = curTextScore;

		if (showExplain) {
			curUid = uid;
			curDocId = docid + baseDocId;
			curMatchTermCnt = matchTermCount;
			curMatchTermOffsets = new int[matchTermCount];
			curMatchTermTfs = new int[matchTermCount];
			curMatchTermIdfs = new float[matchTermCount];
			for (int i = 0; i < matchTermCount; i++) {
				info = matchInfos[i];
				term = info.term;
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
		return curScore;
	}
}
