package com.feiniu.flow;

import java.util.HashMap;

import com.feiniu.connect.FnConnection;
/**
 * data pipe flow model 
 * @author chengwen
 * @version 1.2
 */
public interface Flow {
	
	public void INIT(HashMap<String, Object> connectParams); 
	
	public FnConnection<?> PREPARE(boolean isMonopoly,boolean canSharePipe);
	
	public FnConnection<?> GETSOCKET();
	
	public boolean ISLINK();  

	public void REALEASE(boolean isMonopoly,boolean releaseConn);  
}
