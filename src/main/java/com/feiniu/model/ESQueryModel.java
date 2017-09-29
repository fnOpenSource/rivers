package com.feiniu.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortBuilder;

public class ESQueryModel implements FNQuery<QueryBuilder,SortBuilder,AbstractAggregationBuilder>{
	private QueryBuilder query;
	private List<SortBuilder> sortinfo;
	private int start = 0;
	private int count = 5;
	Map<String, List<String[]>> facetSearchParams;
	List<AbstractAggregationBuilder> facetsConfig = new ArrayList<AbstractAggregationBuilder>();
	private Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>(); 
	private boolean showQueryInfo = false;
	private boolean needCorpfuncCnt = false;
	private boolean cacheRequest = true;
	private Set<Integer> excludeSet;
	private String type;
	private String fl="";
	private String fq="";
	
	public ESQueryModel() {
		
	}
	
	public ESQueryModel(QueryBuilder query, List<SortBuilder> sortinfo, int start,
			int count) {
		super();
		this.query = query;
		this.sortinfo = sortinfo;
		this.start = start;
		this.count = count;
	} 
	
	@Override
	public QueryBuilder getQuery() {
		if (query != null && attrQueryMap.size() > 0)
		{			
			BoolQueryBuilder bQuery = QueryBuilders.boolQuery();
			bQuery.must(query);
			for(QueryBuilder q : attrQueryMap.values()){
				bQuery.must(q);
			}
			return bQuery;
		}
		return query;
	}
	
	@Override
	public void setQuery(QueryBuilder query) {
		this.query = query;
	} 
	
	@Override
	public List<SortBuilder> getSortinfo() {
		return sortinfo;
	}
	public void setSorts(List<SortBuilder> sortinfo) {
		this.sortinfo = sortinfo;
	}
	
	@Override
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	@Override
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	@Override
	public Map<String, List<String[]>> getFacetSearchParams() {
		return facetSearchParams;
	}
	public void setFacetSearchParams(Map<String, List<String[]>> facetSearchParams) {
		this.facetSearchParams = facetSearchParams;
	}

	@Override
	public List<AbstractAggregationBuilder> getFacetsConfig() {
		if (facetSearchParams != null)
		{
			for(Map.Entry<String, List<String[]>> e : facetSearchParams.entrySet())
			{
				TermsBuilder aggr = AggregationBuilders.terms(e.getKey()).field(e.getKey())
						.size(1000).order(Order.count(false));
				facetsConfig.add(aggr);
			}
		}
		return facetsConfig;
	}

	public void setFacetsConfig(List<AbstractAggregationBuilder >  facetsConfig) {
		this.facetsConfig = facetsConfig;
	}
	
	@Override
	public Map<String, QueryBuilder> getAttrQueryMap() {
		return attrQueryMap;
	}

	public void setAttrQueryMap(Map<String, QueryBuilder> attrQueryMap) {
		this.attrQueryMap = attrQueryMap;
	} 
	
	@Override
	public boolean isShowQueryInfo() {
		return this.showQueryInfo;
	}

	@Override
	public void setShowQueryInfo(boolean isshow) {
		this.showQueryInfo = isshow;
	}

	public boolean isNeedCorpfuncCnt() {
		return needCorpfuncCnt;
	}

	public void setNeedCorpfuncCnt(boolean needCorpfuncCnt) {
		this.needCorpfuncCnt = needCorpfuncCnt;
	}

	public Set<Integer> getExcludeSet() {
		return excludeSet;
	}

	public void setExcludeSet(Set<Integer> excludeSet) {
		this.excludeSet = excludeSet;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	@Override
	public boolean cacheRequest() {
		return cacheRequest;
	}

	public void setCacheRequest(boolean cacheRequest) {
		this.cacheRequest = cacheRequest;
	}

	@Override
	public Map<String, QueryBuilder> getEveryAttrQueriesMap() {
		Map<String, QueryBuilder> retMap = new HashMap<String, QueryBuilder>();
		if (query != null && attrQueryMap.size() > 0){
			for(Map.Entry<String, QueryBuilder> e : attrQueryMap.entrySet()){
				BoolQueryBuilder bQuery = QueryBuilders.boolQuery();
				bQuery.must(query);
				for(String key : attrQueryMap.keySet()){
					if (e.getKey().equals(key))
						continue;
					bQuery.must(attrQueryMap.get(key));
				}
				retMap.put(e.getKey(), bQuery);
				
			}
		}
		return retMap;
	}

	@Override
	public String getFl() {
		return this.fl;
	}

	@Override
	public void setFl(String fl) {
		this.fl = fl;
	}

	@Override
	public String getFq() { 
		return fq;
	}

	@Override
	public void setFq(String fq) {
		this.fq = fq;
	}
 
}
