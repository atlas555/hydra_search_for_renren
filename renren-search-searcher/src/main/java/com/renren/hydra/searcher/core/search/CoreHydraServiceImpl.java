package com.renren.hydra.searcher.core.search;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopFieldDocs;

import proj.zoie.api.ZoieIndexReader;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.ResultMerger;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.HydraResult;
import com.renren.hydra.search.filter.SearchFilterChain;
import com.renren.hydra.search.pretreat.ISearchPretreater;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.searcher.core.HydraCore;
import com.renren.hydra.util.HydraLong;
import com.renren.hydra.util.SearchFlowFactory;

public class CoreHydraServiceImpl extends AbstractHydraCoreService {

	private static final Logger logger = Logger
			.getLogger(CoreHydraServiceImpl.class);

	private ResultMerger _resultMerger = null;
	private Schema _schema;
	private ISearchPretreater searchPretreater;

	public CoreHydraServiceImpl(HydraCore core) {
		super(core);
		_schema = core.getSchema();
		_resultMerger = new ResultMerger();
		searchPretreater = SearchFlowFactory.createSearchPretreater(_schema);
	}

	private HydraResult search(HydraIndexSearcher searcher, HydraRequest req,
			int partition) throws Exception {
		logger.debug("start to search in partition " + partition);

		int count = req.getCount();
		if (count < 0) {
			throw new IllegalArgumentException("result count must be > 0: "
					+ count);
		}

		HydraResult result = new HydraResult();

		searcher.setPartitionId(partition);
		searcher.setNeedExplain(req.isShowExplanation());
		searcher.setFriendsInfo(req.getFriendsInfoFV());
		searcher.setUserInfo(req.getUserInfo());

		Query query = this.searchPretreater.queryPretreat(req);
		Sort sort = this.searchPretreater.SortPretreat(req);
		SearchFilterChain searchFilterChain = this.searchPretreater.filterPretreat(req);
		TopFieldDocs docs = searcher.search(count);
		if (docs.scoreDocs == null)
			return getEmptyResultInstance(null);
		int numHits = docs.scoreDocs.length;
		HydraScoreDoc[] hits = new HydraScoreDoc[numHits];

		for (int i = 0; i < numHits; ++i) {
			hits[i] = (HydraScoreDoc) docs.scoreDocs[i];
		}

		result.setHits(hits);

		IndexReader indexReader = searcher.getIndexReader();
		int maxDoc = indexReader.maxDoc();
		int numDocs = indexReader.numDocs();
		if(logger.isDebugEnabled()){
			logger.debug("in partition: " + partition + ", NumHits: " + numHits
				+ ", TotalDocs: " + docs.totalHits + ", TotalDocsInIndex: "
				+ numDocs + ", MaxDocInIndex: " + maxDoc);
		}
		result.setNumHits(numHits);
		result.setTotalDocs(docs.totalHits);

		result.setTid(req.getTid());

		Query parsedQ = req.getQuery();
		result.setParsedQuery(parsedQ.toString());

		return result;
	}

	@Override
	public HydraResult handlePartitionedRequest(final HydraRequest request,
			List<ZoieIndexReader<IndexReader>> readerList, int partition)
			throws Exception {
		logger.debug("handle partitioned request for partition " + partition);
		MultiReader reader = new MultiReader(
				readerList.toArray(new IndexReader[readerList.size()]), false);
		ConcurrentHashMap<HydraLong, OnlineAttributeData> attrDataTable = _core
				.getAttrDataTable(partition);
		ConcurrentHashMap<HydraLong, OfflineAttributeData> offAttrDataTable = _core
				.getOffAttrDataTable(partition);
		HydraIndexSearcher searcher = new HydraIndexSearcher(_schema, reader,
				request.getUserId(), request.getSearchType(),this.searchPretreater,request,
				readerList, attrDataTable, offAttrDataTable,
				_core.getConstantFilterChain(partition));

		return search(searcher, request, partition);
	}

	@Override
	public HydraResult mergePartitionedResults(HydraRequest r,
			List<HydraResult> resultList) {
		return _resultMerger.merge(r, resultList);
	}
}
