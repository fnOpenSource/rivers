package com.feiniu.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class FNDataUnit {
	
	Map<String,Object> internalMap = new LinkedHashMap<String, Object>();
	
	public static FNDataUnit getInstance(){
		return new FNDataUnit();
	}
	
	public void addObject(String key, Object o){
		internalMap.put(key, o);
	}
	
	Map<String , Object> getContent(){
		return internalMap;
	}
}
