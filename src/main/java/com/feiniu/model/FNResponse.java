package com.feiniu.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;

public class FNResponse {

	protected Map<String, String> params = new HashMap<String, String>();
	protected Map<String, String> parsedParams = new HashMap<String, String>(); 
	protected FNResultSet result = null;
	private long startTime = 0;
	private long endTime = 0;
	private long duration = 0; 
	private String index = "";
	private String error_info = "";

	public static FNResponse getInstance() {
		return new FNResponse();
	}

	public void setError_info(String err) {
		this.error_info = err;
	}  

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params, NodeConfig prs) {
		if (prs != null) {
			for (Map.Entry<String, String> entry : params.entrySet()) {
				this.params.put(entry.getKey(), entry.getValue());
			}
		}
	}   

	public FNResultSet getResult() {
		return result;
	}

	public void setResult(FNResultSet result) {
		this.result = result;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
		this.duration = this.endTime - this.startTime;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> res = new LinkedHashMap<String, Object>();
		Map<String, Object> rsp = new LinkedHashMap<String, Object>();
		res.put("request", params);
		rsp.put("err_no", this.error_info.length() > 0 ? "500" : "0");
		rsp.put("err_msg", this.error_info);
		rsp.put("env", GlobalParam.run_environment);
		rsp.put("searcher", String.valueOf(getIndex()));
		rsp.put("duration", String.valueOf(getDuration()) + "ms");
		 
		Map<String, String> paramsMap = new HashMap<String, String>();
		paramsMap.putAll(params);
		if (result != null) {
			rsp.put("results", getResumltMap());
		}
		res.put("response", rsp);
		return res;
	} 

	public String toJson() {
		return JSON.toJSONString(toMap());
	} 

	private Map<String, Object> getResumltMap() {
		Map<String, Object> contentMap = new LinkedHashMap<String, Object>();
		contentMap.put("total", result.getTotalHit()); 
		List<Object> objList = new ArrayList<Object>();
		for (FNDataUnit unit : result.getUnitSet()) {
			objList.add(unit.getContent());
		}
		if (objList.size() > 0)
			contentMap.put("list", objList); 
		if (result.getFacetMap().size() > 0)
			contentMap.put("facet", result.getFacetMap());  
		if (result.getQueryDetail() != null)
			contentMap.put("query", result.getQueryDetail()); 
		if (result.getExplainInfo() != null)
			contentMap.put("explain", result.getExplainInfo()); 
		return contentMap;
	}

}
