package com.feiniu.searcher.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilder;

import com.alibaba.fastjson.JSON;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.model.FNDataUnit;
import com.feiniu.model.FNQuery;
import com.feiniu.model.FNResultSet;
import com.feiniu.model.param.FNParam;
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
			Client conn = (Client) FC.getConnection();
			int start = fq.getStart();
			int count = fq.getCount();
			Map<String, List<String[]>> facetSearchParams = fq
					.getFacetSearchParams();
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
			if (response.getAggregations() != null) {
				Map<String, Aggregation> aggrMap = response.getAggregations()
						.asMap();
				if (aggrMap != null && aggrMap.size() > 0)
					ESFacetResult(res, aggrMap, null, NodeConfig, facetSearchParams);
			} 
			 
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
					&& fq.getAttrQueryMap().size() > 0) {
				Map<String, QueryBuilder> attrQueris = (Map<String, QueryBuilder>) fq
						.getEveryAttrQueriesMap();
				for (Map.Entry<String, QueryBuilder> e : attrQueris.entrySet()) {
					SearchResponse response_selected = getSearchResponse(
							conn,e.getValue(), returnFields, index, 0, 1, null,
							facetBuilders,fq,res);
					if (response_selected.getAggregations() != null) {
						Map<String, Aggregation> aggrMap_selected = response_selected
								.getAggregations().asMap();
						if (aggrMap_selected != null && aggrMap_selected.size() > 0)
							ESFacetResult(res, aggrMap_selected, e.getKey(),
									NodeConfig, facetSearchParams);
					}
				}
			} 
		}catch(Exception e){ 
			throw new FNException("Search data from ES exception!"+e.getMessage());
		}finally{
			CLOSED(FC); 
		} 
		return res;
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

	private void ESFacetResult(FNResultSet ret,
			Map<String, Aggregation> facetsCollector, String specified,
			NodeConfig prs, Map<String, List<String[]>> facetSearchParams) {
		Map<String, Map<String, Integer>> facetMap = ret.getFacetMap();
		FNParam pr = null;
		Set<String> fieldSet = null;

		if (specified != null)
			pr = prs.getParamMap().get(specified);

		if (pr != null && pr.getFields() != null) {
			String[] fields = pr.getFields().split(",");
			fieldSet = new HashSet<String>();
			for (String f : fields)
				fieldSet.add(f);
			if (pr.getName().endsWith(GlobalParam.FACET_SUFFIX))
				fieldSet.add(pr.getName().substring(
						0,
						pr.getName().length()
								- GlobalParam.FACET_SUFFIX.length()));
		}

		for (Map.Entry<String, Aggregation> e : facetsCollector.entrySet()) {
			String nodeStr = e.getKey();
			if ((fieldSet != null && !fieldSet.contains(nodeStr))
					|| !facetSearchParams.containsKey(nodeStr))
				continue;

			Map<String, Integer> nodeMap = new LinkedHashMap<String, Integer>();
			Collection<Terms.Bucket> buckets = ((Terms) e.getValue())
					.getBuckets();
			for (Terms.Bucket entry : buckets) {
				nodeMap.put((String) entry.getKey(), (int) entry.getDocCount());
			}
			if (nodeMap.size() > 0)
				facetMap.put(nodeStr, nodeMap);
		}
	}
}
