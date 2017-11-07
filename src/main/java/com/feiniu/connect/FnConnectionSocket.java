package com.feiniu.connect; 

import java.util.HashMap;

public class FnConnectionSocket{
	
	protected HashMap<String, Object> connectParams = null;
	
	private boolean isShare = false;
	 
	public void init(HashMap<String, Object> ConnectParams) {
		this.connectParams = ConnectParams; 
	}
 
	public boolean isShare() { 
		return this.isShare;
	}
 
	public void setShare(boolean share) {
		this.isShare = share;
	}  
}
