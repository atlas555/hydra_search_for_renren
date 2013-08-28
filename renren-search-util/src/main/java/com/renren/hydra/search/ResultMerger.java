package com.renren.hydra.search;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import org.apache.lucene.util.PriorityQueue;

import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.HydraSortScoreDocPriorityQueue;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.HydraSortScoreDocPriorityQueue;
import com.renren.hydra.search.scorer.IDocScoreAble;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.HydraResult;

public class ResultMerger
{
	private final static Logger logger = Logger.getLogger(ResultMerger.class.getName());
	private class HydraHitItem implements IDocScoreAble {
		private HydraScoreDoc _hit;
		private int _id;
		
		public HydraHitItem(HydraScoreDoc hit, int id) {
			_hit = hit;
			_id = id;
		}

		public HydraScoreDoc getHit() {
			return _hit;
		}

		public int getId() {
			return _id;
		}

		@Override
		public long getUID() {
			return _hit.getUID();
		}

		@Override
		public float getScore() {
			return _hit.getScore();
		}

		@Override
		public Comparable getFieldValue(int index) {
			assert _hit instanceof HydraScoreDoc;

			HydraScoreDoc scoreDoc = (HydraScoreDoc) _hit;
			return scoreDoc.getFieldValue(index);
		}	
		
	}
	
	
	private final class HydraHitMerger {
		private ArrayList<Iterator<HydraScoreDoc>> _iterators = null;
		public PriorityQueue<HydraHitItem> _pq = null;
		private boolean _needExplain = false;

		public boolean is_needExplain() {
			return _needExplain;
		}

		public void set_needExplain(boolean _needExplain) {
			this._needExplain = _needExplain;
		}
		
		public void init(ArrayList<Iterator<HydraScoreDoc>> iterators, Sort sort) {
			_iterators = iterators;
			logger.debug(_iterators.size()+" lists need to be merged");
		
			_pq = new HydraSortScoreDocPriorityQueue<HydraHitItem>(iterators.size(), sort==null?null:sort.getSort(),true); 

			for (int i = 0; i < _iterators.size(); i++) {
				if (_iterators.get(i).hasNext()) {
					_pq.add(new HydraHitItem(_iterators.get(i).next(), i));
				}
			}
		}

		public List<HydraScoreDoc> merge(int resultCount) {
			int count = 0;
			List<HydraScoreDoc> result = new ArrayList<HydraScoreDoc>(resultCount); 
			while (_pq.size() > 0) {
				HydraHitItem item = _pq.pop();
				result.add(item.getHit());
				count++;
				if (count == resultCount) {
					break;
				}

				int id = item.getId();
				logger.debug("get from iterator id :"+id);
				if (_iterators.get(id).hasNext()) {
					_pq.add(new HydraHitItem(_iterators.get(id).next(), id));
				}
			}
			return result;
		}	
	}

	/**
	 * 根据排序类型，将每个partition的结果集合进行合并。其中HydraResult是HydraHit[]的封装。
	 * @param req
	 * @param results
	 * @return
	 */
	public HydraResult merge(HydraRequest req, Collection<HydraResult> results) {
		logger.debug("start to merge hydra result");
		ArrayList<Iterator<HydraScoreDoc>> iteratorList = new ArrayList<Iterator<HydraScoreDoc>>(results.size());
		int numHits = 0;
		int totalDocs = 0;
    
		for (HydraResult res : results)
		{
			if (res.isEmpty()) {
				continue;
			}
			numHits += res.getNumHits();
			totalDocs += res.getTotalDocs();
			
			iteratorList.add(Arrays.asList(res.getHits()).iterator());
		}

		HydraHitMerger merger = new HydraHitMerger();
		merger.set_needExplain(req.isShowExplanation());

		merger.init(iteratorList,req.getSort());
		
		List<HydraScoreDoc> mergedList = merger.merge(req.getCount());
		HydraScoreDoc[] hits = mergedList.toArray(new HydraScoreDoc[mergedList.size()]);
		
		HydraResult merged = new HydraResult();
		merged.setHits(hits);
		merged.setNumHits(numHits);
		merged.setTotalDocs(totalDocs);
		merged.setParsedQuery(req.getQuery().toString());
		return merged;
	}
}

