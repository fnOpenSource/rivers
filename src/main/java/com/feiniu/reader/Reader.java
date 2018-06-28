package com.feiniu.reader;

import com.feiniu.model.WriteUnit; 
/**
 * define data scan interface
 * @author chengwen
 * @version 1.0 
 */
public interface Reader<T> {
	 
	public void init(T rs); 
	
	public boolean status();
	
	public WriteUnit getLineData();
	
	public String getIncrementColumn();
	
	public String getkeyColumn();
	
	public boolean nextLine();
	
	public void close();
	
	public String getScanStamp();
	
	public String getMaxId();
}
