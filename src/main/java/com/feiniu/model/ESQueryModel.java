package com.feiniu.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.searcher.flow.ESQueryBuilder;
import com.feiniu.util.SearchParamUtil;

import org.elasticsearch.script.Script;

public class ESQueryModel implements FNQuery<QueryBuilder,SortBuilder,AbstractAggregationBuilder>{
	private QueryBuilder query;
	private List<SortBuilder> sortinfo;
	private int start = 0;
	private int count = 5;
	Map<String,List<String[]>> facetSearchParams;
	List<AbstractAggregationBuilder> facetsConfig = new ArrayList<AbstractAggregationBuilder>();
	private Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>(); 
	private boolean showQueryInfo = false;
	private boolean needCorpfuncCnt = false;
	private boolean cacheRequest = true;
	private Set<Integer> excludeSet;
	private String type;
	private String fl="";
	private String fq="";
	private String facet_ext="";
	private String requesthandler="";
	
	public static ESQueryModel getInstance(FNRequest request, Analyzer analyzer,NodeConfig nodeConfig) {
		ESQueryModel eq = new ESQueryModel(); 
		eq.setSorts(SearchParamUtil.getSortField(request, nodeConfig));
		eq.setFacetSearchParams(SearchParamUtil.getFacetParams(request, nodeConfig));
		if(request.getParam("facet_ext")!=null){
			eq.setFacet_ext(request.getParams().get("facet_ext"));
		} 
		Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>();
		BoolQueryBuilder query = ESQueryBuilder.buildBooleanQuery(request,
				nodeConfig, analyzer, attrQueryMap);
		eq.setQuery(query);
		eq.setAttrQueryMap(attrQueryMap);
		return eq;
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
	public Map<String,List<String[]>> getFacetSearchParams() {
		return facetSearchParams;
	}
	public void setFacetSearchParams(Map<String,List<String[]>> facetSearchParams) {
		this.facetSearchParams = facetSearchParams;
	}
	
	public void setFacet_ext(String facet_ext) {
		this.facet_ext = facet_ext;
	}

	
	@Override
	public List<AbstractAggregationBuilder> getFacetsConfig() {
		if (facetSearchParams != null)
		{ 
			for(Map.Entry<String,List<String[]>> e : facetSearchParams.entrySet())
			{	 
				int i=0;
				AbstractAggregationBuilder  agg = null ;
				for(String[] strs:e.getValue()) {
					if(i==0) {
						agg = genAgg(strs[0],strs[1],strs[2],true);
					}else {
						((AggregationBuilder<?>) agg).subAggregation(genAgg(strs[0],strs[1],strs[2],false));
					}
					i++; 
				}  
				facetsConfig.add(agg);
			}
		}
		return facetsConfig;
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

	@Override
	public Map<String, String> getFacetExt() {
		Map<String, String> ext = new HashMap<String, String>();
		if(this.facet_ext.length()>0){ 
			for(String str:this.facet_ext.split(",")){
				String[] tmp = str.split(":");
				ext.put(tmp[0], tmp[1]);
			}
		} 
		return ext;
	}
	
	@Override
	public void setRequestHandler(String handler) {
		this.requesthandler = handler; 
	}

	@Override
	public String getRequestHandler() { 
		return this.requesthandler;
	} 
	
	/**
	 * get Aggregation
	 * @param type  true is main , false is sub
	 * @param name
	 * @param field
	 * @param fun
	 * @return
	 */
	private AbstractAggregationBuilder genAgg(String fun,String name,String field,boolean type) {
		switch (fun) {
			case "cardinality": 
				return AggregationBuilders.cardinality(name).field(field); 
			case "avg":
				return AggregationBuilders.avg(name).field(field); 
			case "sum":
				if(field.contains("(script(")) {
					return AggregationBuilders.sum(name).script(getScript(field)); 
				}else {
					return AggregationBuilders.sum(name).field(field); 
				} 
			case "topHits":
				String[] tmp = field.split(":");
				if(tmp.length>1) { 
					String sortField;
					SortOrder sod;
					if(tmp[1].endsWith(GlobalParam.SORT_ASC)) {
						sortField = tmp[1].substring(0, tmp[1].indexOf(GlobalParam.SORT_ASC));
						sod = SortOrder.ASC;
					}else {
						sortField = tmp[1].substring(0, tmp[1].indexOf(GlobalParam.SORT_DESC));
						sod = SortOrder.DESC;
					}
					return AggregationBuilders.topHits(fun).addSort(sortField, sod).setFrom(Integer.valueOf(name)).setSize(Integer.valueOf(tmp[0]));
				}else {
					return AggregationBuilders.topHits(fun).setFrom(Integer.valueOf(name)).setSize(Integer.valueOf(field));
				} 
		} 
		if(type) {
			Map<String, String> ext = getFacetExt();
			TermsBuilder tb = AggregationBuilders.terms(name);
			if(ext.containsKey("size")) {
				tb.field(field).size(Integer.valueOf(ext.get("size")));
			} 
			if(ext.containsKey("order")) {
				String[] tmp = ext.get("order").split(" ");
				if(tmp.length==2)
					tb.order(Order.aggregation(tmp[0], tmp[1].equals("desc")?false:true));
			}
			return tb;
		}
		return AggregationBuilders.terms(name).field(field);
	}
	
 private org.elasticsearch.script.Script getScript(String str) {
	 str = str.replace("(script(", "");
	 str = str.substring(0, str.length()-2); 
	 Script script = new Script(str); 
	 return script;
 }

}
