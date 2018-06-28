package com.feiniu.model;

import java.util.ArrayList;
import java.util.List;
 
public class SearcherResult {
	private float useTime;
	private String callDateTime; 
	private int totalHit;
	private List<SearcherDataUnit> unitSet;
	private Object facetInfo=null;   
	private Object queryDetail = null;
	private Object explainInfo;

	public SearcherResult() {
		unitSet = new ArrayList<SearcherDataUnit>();
	}

	public List<SearcherDataUnit> getUnitSet() {
		return unitSet;
	}

	public void setUnitSet(List<SearcherDataUnit> unitSet) {
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

	public Object getFacetInfo() {
		return facetInfo;
	}

	public void setFacetInfo(Object facetInfo) {
		this.facetInfo = facetInfo;
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