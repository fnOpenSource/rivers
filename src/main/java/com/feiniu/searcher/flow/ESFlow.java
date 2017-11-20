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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.sort.SortBuilder;

import com.alibaba.fastjson.JSON;
import com.feiniu.connect.ESConnector;
import com.feiniu.connect.FnConnection;
import com.feiniu.model.FNDataUnit;
import com.feiniu.model.FNQuery;
import com.feiniu.model.FNResultSet;
import com.feiniu.model.param.WriteParam;
import com.feiniu.util.FNException;

public class ESFlow extends SearcherFlowSocket { 

	public static ESFlow getInstance(HashMap<String, Object> connectParams) {
		ESFlow o = new ESFlow();
		o.INIT(connectParams);
		return o;
	} 

	@SuppressWarnings("unchecked")
	@Override
	public FNResultSet Search(FNQuery<?, ?, ?> fq, String index)
			throws FNException {
		FnConnection<?> FC = PULL(true);
		FNResultSet res = new FNResultSet();
		try{
			ESConnector ESC = (ESConnector) FC.getConnection(true);
			Client conn = ESC.getClient();
			int start = fq.getStart();
			int count = fq.getCount(); 
			List<SortBuilder> sortFields = (List<SortBuilder>) fq.getSortinfo();
			QueryBuilder qb = (QueryBuilder) fq.getQuery();
			List<AbstractAggregationBuilder> facetBuilders = (List<AbstractAggregationBuilder>) fq
					.getFacetsConfig(); 
			
			List<String> returnFields = new ArrayList<String>();
			if (fq.getFl().length() > 0) {
				for (String s : fq.getFl().split(",")) {
					returnFields.add(s);
				}
			} else {
				Map<String, WriteParam> tmpFields = NodeConfig.getWriteParamMap();
				for (Map.Entry<String, WriteParam> e : tmpFields.entrySet()) {
					if (e.getValue().getStored().equalsIgnoreCase("true"))
						returnFields.add(e.getKey());
				}
			}

			SearchResponse response = getSearchResponse(conn,qb, returnFields, index,
					start, count, sortFields, facetBuilders, fq,res);
			SearchHits searchHits = response.getHits();
			res.setTotalHit((int) searchHits.getTotalHits());  
			SearchHit[] hits = searchHits.getHits();  
			 
			for (SearchHit h:hits) {
				Map<String, SearchHitField> fieldMap = h.getFields(); 
				FNDataUnit u = FNDataUnit.getInstance();
				for (Map.Entry<String, SearchHitField> e : fieldMap.entrySet()) {
					String name = e.getKey();
					WriteParam param = NodeConfig.getWriteParamMap().get(name);
					SearchHitField v = e.getValue();  
					if (param.getSeparator() != null) { 
						u.addObject(name, v.getValues());
					} else {
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
				Map<String, Aggregation> aggrMap = response.getAggregations().getAsMap(); 
				Map<String, Map<String, String>> facetRes = new HashMap<String, Map<String,String>>(); 
				for (Entry<String, Aggregation> r : aggrMap.entrySet()) { 
					facetRes.put(r.getKey(), getFacets((String) (fq.getFacetExt().containsKey("type")?fq.getFacetExt().get("type"):"terms"),r.getValue()));
				}
				res.setFacetMap(facetRes);
			} 
		}catch(Exception e){ 
			throw new FNException("Search data from ES exception!"+e.getLocalizedMessage());
		}finally{
			CLOSED(FC,false); 
		} 
		return res;
	}
	
	private Map<String, String> getFacets(String type,Aggregation agg){
		Map<String, String> r = new HashMap<String, String>();
		switch (type) {
		case "cardinality":
			Cardinality v = (Cardinality) agg;
			r.put("value", v.getValue()+"");
			break;

		default:
			break;
		}
		return r; 
	}
	 
	private SearchResponse getSearchResponse(Client conn,QueryBuilder qb,
			List<String> returnFields, String instance, int start, int count,
			List<SortBuilder> sortFields,
			List<AbstractAggregationBuilder> facetBuilders,FNQuery<?, ?, ?> fq,FNResultSet res) {
		SearchRequestBuilder request = conn.prepareSearch(instance).setPreference("_replica_first");
		request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		request.setQuery(qb);
		request.setSize(count);
		request.setFrom(start);

		if (sortFields != null)
			for (SortBuilder s : sortFields) {
				request.addSort(s);
			}
 
		if (facetBuilders != null)
			for (AbstractAggregationBuilder facet : facetBuilders) {
				request.addAggregation(facet);
			}

		request.addFields(returnFields.toArray(new String[returnFields.size()])); 
	 
		if (fq.isShowQueryInfo()) { 
			res.setQueryDetail(JSON.parse(request.toString()));
			request.setExplain(true); 
		} 
		SearchResponse response = request.execute().actionGet();
		return response;
	}
 
}
