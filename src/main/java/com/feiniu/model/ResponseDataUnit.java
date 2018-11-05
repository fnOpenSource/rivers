package com.feiniu.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class ResponseDataUnit {
	
	Map<String,Object> internalMap = new LinkedHashMap<String, Object>();
	
	public static ResponseDataUnit getInstance(){
		return new ResponseDataUnit();
	}
	
	public void addObject(String key, Object o){
		internalMap.put(key, o);
	}
	
	public Map<String , Object> getContent(){
		return internalMap;
	}
}
