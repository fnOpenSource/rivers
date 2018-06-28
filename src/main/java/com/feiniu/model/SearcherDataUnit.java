package com.feiniu.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class SearcherDataUnit {
	
	Map<String,Object> internalMap = new LinkedHashMap<String, Object>();
	
	public static SearcherDataUnit getInstance(){
		return new SearcherDataUnit();
	}
	
	public void addObject(String key, Object o){
		internalMap.put(key, o);
	}
	
	Map<String , Object> getContent(){
		return internalMap;
	}
}
