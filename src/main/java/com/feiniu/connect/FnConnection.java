package com.feiniu.connect;

import java.util.HashMap;
 
public interface FnConnection<T>{ 
	
	public void init(HashMap<String, Object> ConnectParams);
	
	public boolean connect(); 
	
	public T getConnection();
	
	public boolean status();
	
	public boolean free();
	
	public boolean isShare();
	
	public void setShare(boolean share);
}
