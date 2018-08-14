package com.feiniu.model.param;

import java.util.HashMap;

import com.feiniu.config.GlobalParam.DATA_TYPE;

/**
 * seq for series data position define
 * @author chengwen
 * @version 1.0 
 */
public interface WarehouseParam {
	
	public String[] getSeq();
	
	public void setSeq(String seqs);
	
	public DATA_TYPE getType();
	
	public String getHandler(); 
	
	public String getPoolName(String seq);
	
	public HashMap<String, Object> getConnectParams(String seq);	
}
