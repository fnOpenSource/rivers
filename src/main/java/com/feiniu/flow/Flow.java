package com.feiniu.flow;

import java.util.HashMap;

import com.feiniu.connect.FnConnection;
/**
 * data pipe flow model
 * first INIT and get connect socket (GETSOCKET)
 * build connect (MONOPOLY/LINK) start use,when finished use Dismantling pipe (REALEASE)
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
