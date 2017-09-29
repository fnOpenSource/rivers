package com.feiniu.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
public class FNResultSet {
	private float useTime;
	private String callDateTime; 
	private int totalHit;
	private List<FNDataUnit> unitSet;
	private Map<String, Map<String, Integer>> facetMap;  
 	private Object queryDetail = null;
	private Object explainInfo;

	public FNResultSet() {
		unitSet = new ArrayList<FNDataUnit>();
		facetMap = new HashMap<String, Map<String, Integer>>();   
	}

	public List<FNDataUnit> getUnitSet() {
		return unitSet;
	}

	public void setUnitSet(List<FNDataUnit> unitSet) {
		this.unitSet = unitSet;
	}

	public float getUseTime() {
		return useTime;
	}

	public void setUseTime(float useTime) {
		this.useTime = useTime;
	}

	public String getCallDateTime() {
		return callDateTime;
	}

	public void setCallDateTime(String callDateTime) {
		this.callDateTime = callDateTime;
	}

	public int getTotalHit() {
		return totalHit;
	}

	public void setTotalHit(int totalHit) {
		this.totalHit = totalHit;
	}

	public Map<String, Map<String, Integer>> getFacetMap() {
		return facetMap;
	}

	public void setFacetMap(Map<String, Map<String, Integer>> facetMap) {
		this.facetMap = facetMap;
	}   

	public Object getQueryDetail() {
		return queryDetail;
	}

	public void setQueryDetail(Object queryDetail) {
		this.queryDetail = queryDetail;
	}

	public Object getExplainInfo() {
		return explainInfo;
	}

	public void setExplainInfo(Object explainInfo) {
		this.explainInfo = explainInfo;
	} 
}