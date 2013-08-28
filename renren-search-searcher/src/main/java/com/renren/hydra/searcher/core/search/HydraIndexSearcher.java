package com.renren.hydra.searcher.core.search;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ReaderUtil;

import proj.zoie.api.ZoieIndexReader;

import com.renren.hydra.attribute.OfflineAttributeData;
import com.renren.hydra.attribute.OnlineAttributeData;
import com.renren.hydra.config.schema.Schema;
import com.renren.hydra.search.HydraRequest;
import com.renren.hydra.search.filter.Filter;
import com.renren.hydra.search.filter.FriendFilter;
import com.renren.hydra.search.filter.OwnerFilter;
import com.renren.hydra.search.filter.SearchFilter;
import com.renren.hydra.search.filter.SearchFilterChain;
import com.renren.hydra.search.pretreat.ISearchPretreater;
import com.renren.hydra.search.scorer.HydraScoreDoc;
import com.renren.hydra.search.scorer.HydraScorer;
import com.renren.hydra.search.similarity.HydraSimilarity;
import com.renren.hydra.search.sort.Sort;
import com.renren.hydra.search.sort.SortField;
import com.renren.hydra.searcher.core.search.filter.ConstantFilterChain;
import com.renren.hydra.util.HydraLong;
import com.renren.hydra.util.SearchFlowFactory;
import com.renren.hydra.util.StopWordFilter;
import com.renren.hydra.client.SearchType;

public class HydraIndexSearcher extends IndexSearcher {
	private static final Logger logger = Logger
			.getLogger(HydraIndexSearcher.class);
	private boolean _needExplain = false;
	private int _partitionId = -1;
	private ConcurrentHashMap<HydraLong, OnlineAttributeData> _attrDataTable;
	private ConcurrentHashMap<HydraLong, OfflineAttributeData> _offAttrDataTable;
	private ConstantFilterChain _constantFilterChain;
	private Schema _schema;
	private int _userId;
	private SearchType _searchType;
	private Map<Integer, Map<MutableInt, Short>> _friendsInfo; 
	private Map<String, String> _userInfo;
	private HydraLong uidTmp;
	private List<ZoieIndexReader<IndexReader> > zoieIndexReaders;
	private int[] starts;
	private int subIdx;
	private int subdocid;
	private ISearchPretreater searchPretreater;
	private HydraRequest request;
	
	public HydraIndexSearcher(Schema schema, IndexReader r,
			int userId, SearchType searchType,ISearchPretreater searchPretreater,HydraRequest request,
			List<ZoieIndexReader<IndexReader>> readerList,
			ConcurrentHashMap<HydraLong, OnlineAttributeData> attrDataTable,
			ConcurrentHashMap<HydraLong, OfflineAttributeData> offAttrDataTable,
			ConstantFilterChain constantFilterChain) {
		super(r);
		_attrDataTable = attrDataTable;
		_offAttrDataTable = offAttrDataTable;
		this._constantFilterChain = constantFilterChain;
		this._schema = schema;
		this._userId = userId;
		this._searchType = searchType;
		this._friendsInfo = null;
		this._userInfo = null;
		this.uidTmp= new HydraLong(0L);
		this.zoieIndexReaders = readerList;
		this.searchPretreater = searchPretreater;
		this.request = request;
		if(this.zoieIndexReaders!=null)
			initZoieSubReaders();
	}
	
	public void initZoieSubReaders(){
		int size = zoieIndexReaders.size();
		this.starts = new int[size+1];
		int maxDoc = 0;
		for(int i=0;i<size;++i){
			starts[i] = maxDoc;
			maxDoc+= zoieIndexReaders.get(i).maxDoc();
		}
		starts[size] = maxDoc;
	}
	
	public void fillAttribute(HydraScoreDoc scoreDoc, OnlineAttributeData onlineAttributeData, 
			OfflineAttributeData offlineAttributeData){
		if(onlineAttributeData!=null){
			scoreDoc.setOnlineAttributeData(onlineAttributeData.getAttributeDataMap());
		}else{
			scoreDoc.setOnlineAttributeData(this._schema.getDefaultOnlineAttributeDataMap());
			logger.debug("fillAttribute: onlineAttributeData is null");
		}
		if(offlineAttributeData!=null){
			scoreDoc.setOfflineAttributeData(offlineAttributeData.getAttributeDataMap());
		}else
			scoreDoc.setOfflineAttributeData(this._schema.getDefaultOfflineAttributeDataMap());
	}

	public ConcurrentHashMap<HydraLong, OfflineAttributeData> getOffAttrDataTable() {
		return _offAttrDataTable;
	}

	public long getUid(int docid){
		subIdx =  ReaderUtil.subIndex(docid, starts);
		subdocid = docid - starts[subIdx];
		return zoieIndexReaders.get(subIdx).getUID(subdocid);
	}

	public void setUserInfo(Map<String, String> userInfo) {
		_userInfo = userInfo;
	}

	public void setFriendsInfo(Map<Integer, Map<MutableInt, Short>> friendsInfo) {
		_friendsInfo = friendsInfo;
	}

	public void setPartitionId(int partitionId) {
		_partitionId = partitionId;
	}

	public int getPartitionId() {
		return _partitionId;
	}

	public void setNeedExplain(boolean needExplain) {
		_needExplain = needExplain;
	}

	
	public HydraCollector getDocCollector(int nDocs, Sort sort,boolean allDocsQuery){
		HydraCollector collector = null;
		if(null!=sort){
			SortField[] sortFields = sort.getSort();
			if(allDocsQuery)
				sort.setNeedScore(false);
			if(sortFields!=null){
				int numSortField = sortFields.length;
				if(numSortField>1){
					collector = new HydraSortScoreDocCollector(nDocs, sort);
					logger.debug("sort score doc collector");
				}else if(numSortField==1){
					SortField sortField = sortFields[0];
					boolean reverse = sortField.isReverse();
					if(reverse){
						collector = new OneSortFieldHydraScoreDocCollector(nDocs, sortField,sort.isNeedScore(),reverse);
						logger.debug("one sort field reverse score doc collector");
					}else{
						collector = new OneSortFieldHydraScoreDocCollector(nDocs, sortField,sort.isNeedScore());
						logger.debug("one sort field  score doc collector");
					}
				}
			}
		}
		if(collector==null){
			collector = new HydraScoreDocCollector(nDocs,!allDocsQuery);
		}
		return collector;
	}
	
	public SearchFilterChain getSearchFilterChain(Filter filter){
		SearchFilterChain searchFilterChain = null;
		
		if(filter!=null){
			searchFilterChain = new SearchFilterChain(filter);
			logger.debug("filter :"+filter.toString());
		}
		SearchFilter searchFilter = null;
		if(_searchType == SearchType.OnlyUser){
			logger.debug("only user :" + _userId);
			 searchFilter = new OwnerFilter(_userId);
		}else if(_searchType == SearchType.OnlyFriends){
			logger.debug("only friends");
			 searchFilter = new FriendFilter(_friendsInfo.get(1));
		}else{
			searchFilter = null;
		}
		
		if(searchFilter!=null){
			if(searchFilterChain==null)
				searchFilterChain = new SearchFilterChain();
			searchFilterChain.addSearchFilter(searchFilter);
		}
		return searchFilterChain;
	}
	
	public TopFieldDocs search(int nDocs) throws IOException {
		logger.debug("begin execute search");
		nDocs = Math.min(nDocs, maxDoc());
		boolean allDocsQuery = false;
		
		Query query = request.getQuery();
		if(query instanceof MatchAllDocsQuery){
			allDocsQuery = true;
			this._needExplain = false;
		}
		else{
			allDocsQuery = false;
		}
		Sort sort = request.getSort();
		boolean fillAttribute = request.isFillAttribute();
		SearchFilterChain searchFilterChain = null;
		

		query = this.searchPretreater.queryPretreat(request);
		sort = this.searchPretreater.SortPretreat(request);
		searchFilterChain = this.searchPretreater.filterPretreat(request);
			
		Weight weight = createWeight(query);
		HydraCollector collector = getDocCollector(nDocs,sort,allDocsQuery);
		HydraSimilarity similarity = (HydraSimilarity) SearchFlowFactory
				.createSimilarity(_schema, weight.getQuery(), _needExplain);

		collector.setSimilarity(similarity);
		collector.setPartition(_partitionId);
		similarity.setPartitionId(_partitionId);
		similarity.setUserInfo(_userInfo);
		similarity.setFriendsInfo(_friendsInfo);
		
		if(StopWordFilter.filter(request.getQString())){
			collector.setNeedScore(false);
		}

		Map<MutableInt, Short> friends1 = null;
		if(_friendsInfo != null)
			friends1 = _friendsInfo.get(1);

		int totalDocs = 0;
		int baseDocId = 0;

		//SearchFilterChain searchFilterChain = getSearchFilterChain(filter);
		
		logger.debug("Traverse doc list and score.");
		for (int i = 0; i < subReaders.length; i++) {
			collector.setIndexReader(subReaders[i]);
			Scorer scorer =  weight.scorer(subReaders[i],
					false, false);
			if(!allDocsQuery)
				collector.setScorer((HydraScorer)scorer);
			while (true) {
				int localDocId = scorer.nextDoc();
				if (localDocId == DocIdSetIterator.NO_MORE_DOCS) {
					break;
				}

				long uid = getUid(localDocId + baseDocId);
				uidTmp.set(uid);
				
				OnlineAttributeData onlineAttributeData = _attrDataTable
						.get(uidTmp);
				OfflineAttributeData offlineAttributeData = _offAttrDataTable
						.get(uidTmp);

				if(onlineAttributeData==null)
					continue;
				
				// add filter chain here
				if (null != this._constantFilterChain){
					if(this._constantFilterChain.filter(uid,onlineAttributeData,offlineAttributeData,friends1)){
						logger.debug("filter result: " + uid);
						continue;
					}
				}
				
				/*
				 * 不满足条件的被过滤掉，满足条件的进行后续的打分逻辑 
				 */
				if(searchFilterChain!=null){
					if(!searchFilterChain.filter(uid,onlineAttributeData, offlineAttributeData))
						continue;
				}

				try {
					collector.collect(localDocId, baseDocId, uid,
							onlineAttributeData, offlineAttributeData);
				} catch (Exception e) {
					logger.error("in part " + _partitionId
							+ ", fail to collect for docid: " + localDocId
							+ " in subReader " + i + ". error: "
							+ e.getMessage());
					logger.error(ExceptionUtils.getFullStackTrace(e));
					continue;
				}

				totalDocs++;
			}
			baseDocId += subReaders[i].maxDoc();
		}

		ScoreDoc[] scoreDocs = collector.topDocs();
		if(fillAttribute){
			if(scoreDocs!=null){
				for(ScoreDoc scoreDoc : scoreDocs){
					long uid = ((HydraScoreDoc)scoreDoc)._uid;
					uidTmp.set(uid);
					OnlineAttributeData onlineAttributeData = _attrDataTable.get(uidTmp);
					OfflineAttributeData offlineAttributeData = _offAttrDataTable.get(uidTmp);
					fillAttribute((HydraScoreDoc)scoreDoc,onlineAttributeData,offlineAttributeData);
				}
			}
		}

		if (_needExplain) {
			int index = scoreDocs.length - 1;
			logger.debug("Show Explanation:");
			while (index >= 0) {
				logger.debug("Query "
						+ weight.getQuery().toString()
						+ " Explanation: "
						+ ((HydraScoreDoc) scoreDocs[index]).explainationStr);
				index--;
			}
		}

		logger.debug("end search");
		return new TopFieldDocs(totalDocs, scoreDocs, null,
				collector.maxScore());
	}
}
