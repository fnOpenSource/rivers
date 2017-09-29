package com.feiniu.model;

import java.util.List;
import java.util.Map;

public interface FNQuery<T1, T2, T3> {
	public T1 getQuery();

	public void setQuery(T1 query);

	public void setStart(int param);

	public int getStart();

	public void setCount(int param);

	public int getCount();
	
	public String getFl();
	
	public void setFl(String fl);
	
	public String getFq();
	
	public void setFq(String fq); 

	public Map<String, List<String[]>> getFacetSearchParams();
  
	public void setShowQueryInfo(boolean isshow);
	
	public boolean isShowQueryInfo();
	
	public List<T2> getSortinfo();

	public Map<String, T1> getAttrQueryMap();

	public Map<String, T1> getEveryAttrQueriesMap();

	public List<T3> getFacetsConfig(); 

	public boolean cacheRequest();
}
