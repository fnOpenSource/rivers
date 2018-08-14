package com.feiniu.model;

import java.util.concurrent.ConcurrentHashMap;

import com.feiniu.util.Common;

public class RiverState<T> extends ConcurrentHashMap<String,T>{
 
	private static final long serialVersionUID = 7134367712318896122L;
	
	public void set(String instance,String seq,T dt) {
		put(Common.getInstanceName(instance, seq), dt);
	} 
	
	public void set(String instance,T dt) {
		put(instance, dt);
	} 
	
	public T get(String instance,String seq) {
		return get(Common.getInstanceName(instance, seq));
	}
	
	public boolean containsKey(String instance,String seq) {
		return containsKey(Common.getInstanceName(instance, seq));
	}
	 
}
