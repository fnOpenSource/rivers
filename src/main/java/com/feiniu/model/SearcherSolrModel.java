package com.feiniu.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.client.solrj.SolrQuery;

import com.feiniu.config.InstanceConfig;
import com.feiniu.searcher.flow.SolrQueryBuilder;

public class SearcherSolrModel implements SearcherModel<SolrQuery, String, String> {
	private SolrQuery query ; 
	private int start = 0;
	private int count = 5; 
	private boolean showQueryInfo = false;
	private String fq = "";
	private String fl="";
	private boolean cached=false;
	private String requesthandler="select";
	
	Map<String, List<String[]>> facetSearchParams;
	List<String> facetsConfig = new ArrayList<String>();
	private Map<String, SolrQuery> attrQueryMap = new HashMap<String, SolrQuery>(); 
	private List<String> sortinfo;
	
	public static SearcherSolrModel getInstance(SearcherRequest request, Analyzer analyzer,InstanceConfig instanceConfig) {
		SearcherSolrModel sq = new SearcherSolrModel(); 
		sq.setQuery(SolrQueryBuilder.queryBuilder(request, instanceConfig,
				analyzer, new HashMap<String, String>()));
		return sq;
	}
 
	@Override
	public SolrQuery getQuery() {
		return this.query;
	}

	@Override
	public void setQuery(SolrQuery query) {
		this.query = query;
	}

	@Override
	public int getStart() {
		return this.start;
	}

	@Override
	public void setStart(int start) {
		this.start = start;
	}

	@Override
	public int getCount() {
		return this.count;
	}

	@Override
	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public Map<String,List<String[]>> getFacetSearchParams() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getSortinfo() {
		return this.sortinfo;
	}

	public List<String> setSortinfo(List<String> sortinfo) {
		return this.sortinfo = sortinfo;
	}

	@Override
	public boolean isShowQueryInfo() {
		return this.showQueryInfo;
	}

	@Override
	public void setShowQueryInfo(boolean isshow) {
		this.showQueryInfo = isshow;
	}

	@Override
	public Map<String, SolrQuery> getAttrQueryMap() { 
		return this.attrQueryMap;
	}

	@Override
	public Map<String, SolrQuery> getEveryAttrQueriesMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getFacetsConfig() {
		// TODO Auto-generated method stub
		return null;
	} 

	@Override
	public boolean cacheRequest() { 
		return this.cached;
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
		return null;
	}

	@Override
	public void setRequestHandler(String handler) {
		this.requesthandler = handler; 
	}

	@Override
	public String getRequestHandler() { 
		return this.requesthandler;
	} 
}