package com.feiniu.searcher.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import com.alibaba.fastjson.JSON;
import com.feiniu.connect.ESConnector;
import com.feiniu.model.SearcherDataUnit;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.SearcherResult;
import com.feiniu.model.param.TransParam;
import com.feiniu.searcher.SearcherFlowSocket;
import com.feiniu.searcher.handler.Handler;
import com.feiniu.util.FNException;

public class ESFlow extends SearcherFlowSocket {  
	
	private ESConnector ESC;
	
	public static ESFlow getInstance(HashMap<String, Object> connectParams) {
		ESFlow o = new ESFlow();
		o.INIT(connectParams);
		return o;
	} 
	
	@Override
	public boolean LINK() {
		if(this.FC==null) {
			return false;
		}else {
			this.ESC = (ESConnector) this.FC.getConnection(true);
		}
		return true;  
	}

	@SuppressWarnings("unchecked")
	@Override
	public SearcherResult Search(SearcherModel<?, ?, ?> fq, String instance,Handler handler)
			throws FNException {
		GETSOCKET(true);
		SearcherResult res = new SearcherResult();
		if(!LINK())
			return res;
		try{ 
			Client conn = ESC.getClient();
			int start = fq.getStart();
			int count = fq.getCount(); 
			List<SortBuilder<?>> sortFields = (List<SortBuilder<?>>) fq.getSortinfo();
			QueryBuilder qb = (QueryBuilder) fq.getQuery();
			List<AggregationBuilder> facetBuilders = (List<AggregationBuilder>) fq
					.getFacetsConfig(); 
			
			List<String> returnFields = new ArrayList<String>();
			if (fq.getFl().length() > 0) {
				for (String s : fq.getFl().split(",")) {
					returnFields.add(s);
				}
			} else {
				Map<String, TransParam> tmpFields = instanceConfig.getTransParams();
				for (Map.Entry<String, TransParam> e : tmpFields.entrySet()) {
					if (e.getValue().getStored().equalsIgnoreCase("true"))
						returnFields.add(e.getKey());
				}
			} 
			SearchResponse response = getSearchResponse(conn,qb, returnFields, instance,
					start, count, sortFields, facetBuilders, fq,res);
			if(handler==null) {
				addResult(res,response,fq,returnFields);
			}else {
				handler.Handle(res,response,fq,instanceConfig,returnFields);
			} 
		}catch(Exception e){ 
			throw e;
		}finally{
			REALEASE(FC,false); 
		} 
		return res;
	} 
	
	private void addResult(SearcherResult res,SearchResponse response,SearcherModel<?, ?, ?> fq,List<String> returnFields) {
		SearchHits searchHits = response.getHits();
		res.setTotalHit((int) searchHits.getTotalHits());  
		SearchHit[] hits = searchHits.getHits();  
		 
		for (SearchHit h:hits) {
			Map<String, DocumentField> fieldMap = h.getFields(); 
			SearcherDataUnit u = SearcherDataUnit.getInstance();
			for (Entry<String, DocumentField> e : fieldMap.entrySet()) {
				String name = e.getKey();
				TransParam param = instanceConfig.getTransParams().get(name);
				DocumentField v = e.getValue();  
				if (param!=null && param.getSeparator() != null) { 
					u.addObject(name, v.getValues());
				} else if(returnFields.contains(name)) {
					u.addObject(name, String.valueOf(v.getValue()));
				}
			}
			if(fq.isShowQueryInfo()){ 
				u.addObject("_SCORE", h.getExplanation().getValue()); 
				u.addObject("_EXPLAINS", h.getExplanation().toString().replace("", ""));
			}
			res.getUnitSet().add(u);  
		} 
		 
		if (fq.getFacetSearchParams() != null
				&& response.getAggregations() != null) {  
			res.setFacetInfo(JSON.parseObject(response.toString()).get("aggregations"));
		} 
	}
	 
	private SearchResponse getSearchResponse(Client conn,QueryBuilder qb,
			List<String> returnFields, String instance, int start, int count,
			List<SortBuilder<?>> sortFields,
			List<AggregationBuilder> facetBuilders,SearcherModel<?, ?, ?> fq,SearcherResult res) {
		SearchRequestBuilder request = conn.prepareSearch(instance).setPreference("_replica_first");
		request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		request.setQuery(qb);
		request.setSize(count);
		request.setFrom(start);

		if (sortFields != null)
			for (SortBuilder<?> s : sortFields) {
				request.addSort(s);
			}
 
		if (facetBuilders != null)
			for (AggregationBuilder facet : facetBuilders) {
				request.addAggregation(facet);
			}

		request.storedFields(returnFields.toArray(new String[returnFields.size()])); 
	 
		if (fq.isShowQueryInfo()) { 
			res.setQueryDetail(JSON.parse(request.toString()));
			request.setExplain(true); 
		} 
		SearchResponse response = request.execute().actionGet();
		return response;
	}
 
}
