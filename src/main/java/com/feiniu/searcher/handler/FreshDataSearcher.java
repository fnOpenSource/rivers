package com.feiniu.searcher.handler;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.feiniu.model.SearcherDataUnit;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.SearcherResult;
import com.feiniu.model.param.TransParam;
import com.feiniu.searcher.SearcherFlowSocket; 

public class FreshDataSearcher extends SearcherFlowSocket implements Handler{

	@SuppressWarnings("unchecked")
	@Override
	public <T> T Handle(Object... args) {
		if(args.length == 5) {
			SearcherResult res = (SearcherResult) args[0];
			SearchResponse response = (SearchResponse) args[1];
			SearcherModel<?, ?, ?> fq = (SearcherModel<?, ?, ?>) args[2];
			instanceConfig = (com.feiniu.config.InstanceConfig) args[3];			
			List<String> returnFields = (List<String>) args[4];

			SearchHits searchHits = response.getHits();
			res.setTotalHit((int) searchHits.getTotalHits());  
			SearchHit[] hits = searchHits.getHits();  
			 
			for (SearchHit h:hits) {
				Map<String, SearchHitField> fieldMap = h.getFields(); 
				SearcherDataUnit u = SearcherDataUnit.getInstance();
				for (Map.Entry<String, SearchHitField> e : fieldMap.entrySet()) {
					String name = e.getKey(); 
					TransParam param = instanceConfig.getTransParam(name);
					SearchHitField v = e.getValue();  
					if (param!=null && param.getSeparator() != null) { 
						u.addObject(name, v.getValues());
					} else if(returnFields.contains(name)){
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
				JSONObject jo = (JSONObject) JSON.parseObject(response.toString()).get("aggregations");
				jo = (JSONObject) jo.get("aggs_field");
				JSONArray ja = (JSONArray) jo.get("buckets"); 
				JSONArray jb = new JSONArray();
				int start = 0;
				if(fq.getFacetExt().containsKey("start")) {
					start = Integer.parseInt(fq.getFacetExt().get("start"));
				}  
				for(int i=start;i<start+10;i++) {
					if(ja.size()>i) {
						jb.add(ja.get(i));
					}
				} 
				JSONObject _rs = new JSONObject();
				_rs.put("data", jb);
				_rs.put("total", ja.size());
				res.setFacetInfo(_rs);
			}   
		}
		return null;
	} 
}
