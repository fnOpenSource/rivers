package com.feiniu.connect; 

import java.util.HashMap;

import com.feiniu.config.GlobalParam;

public class FnConnectionSocket{
	
	protected HashMap<String, Object> connectParams = null;
	
	private boolean isShare = false;
	
	private double createTime = System.currentTimeMillis();
	
	public void init(HashMap<String, Object> ConnectParams) {
		this.connectParams = ConnectParams; 
	}
 
	public boolean isShare() { 
		return this.isShare;
	}
 
	public void setShare(boolean share) {
		this.isShare = share;
	} 
	
	protected boolean isOutOfTime() {
		if(System.currentTimeMillis()-createTime>GlobalParam.CONNECT_EXPIRED){
			return true;
		}
		return false;
	}
}
