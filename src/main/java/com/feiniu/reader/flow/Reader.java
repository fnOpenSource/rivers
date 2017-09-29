package com.feiniu.reader.flow;

import com.feiniu.model.WriteUnit; 
/**
 * define data scan interface
 * @author chengwen
 * @version 1.0 
 */
public interface Reader<T> {
	 
	public void init(T rs); 
	
	public WriteUnit getLineData();
	
	public String getIncrementColumn();
	
	public String getkeyColumn();
	
	public boolean nextLine();
	
	public void close();
	
	public String getLastUpdateTime();
	
	public String getMaxId();
}
