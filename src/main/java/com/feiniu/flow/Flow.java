package com.feiniu.flow;

import java.util.HashMap;

import com.feiniu.connect.FnConnection;
/**
 * data pipe flow control center
 * @author chengwen
 * @version 1.0 
 */
public interface Flow {
	public void INIT(HashMap<String, Object> connectParams);

	public FnConnection<?> LINK(boolean canSharePipe);

	public void UNLINK(FnConnection<?> FC,boolean releaseConn);
	
	public void MONOPOLY();
}
