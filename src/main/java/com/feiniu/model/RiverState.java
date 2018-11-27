package com.feiniu.model;

import java.util.concurrent.ConcurrentHashMap;

import com.feiniu.util.Common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-22 09:08
 */
public class RiverState<T> extends ConcurrentHashMap<String,T>{
 
	private static final long serialVersionUID = 7134367712318896122L;
	
	public void set(String instance,T dt) {
		put(instance, dt);
	} 
	
	public void set(String instance,String seq,T dt) {
		put(Common.getMainName(instance, seq), dt);
	} 
	
	public void set(String instance,String seq,String tag,T dt) {
		put(Common.getMainName(instance, seq)+tag, dt);
	}  
	
	public T get(String instance,String seq) {
		return get(Common.getMainName(instance, seq));
	}
	
	public T get(String instance,String seq,String tag) {
		return get(Common.getMainName(instance, seq)+tag);
	}
	
	public boolean containsKey(String instance,String seq) {
		return containsKey(Common.getMainName(instance, seq));
	}
	 
}
