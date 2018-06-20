package com.feiniu.model;

import java.util.concurrent.ConcurrentHashMap;

public class RiverModel<T> extends ConcurrentHashMap<String,T>{
 
	private static final long serialVersionUID = 7134367712318896122L;
	
	public void set(String instance,String seq,T dt) {
		put(getKeyName(instance, seq), dt);
	} 
	
	public T get(String instance,String seq) {
		return get(getKeyName(instance, seq));
	}
	
	public boolean containsKey(String instance,String seq) {
		return containsKey(getKeyName(instance, seq));
	}
	
	private String getKeyName(String instance,String seq) {
		return new StringBuffer(instance).append("_").append(seq).toString();
	}
}
