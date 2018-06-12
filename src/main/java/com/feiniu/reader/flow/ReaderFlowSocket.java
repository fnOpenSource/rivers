package com.feiniu.reader.flow;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.flow.Flow;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;
import com.feiniu.reader.handler.Handler;

public class ReaderFlowSocket<T> implements Flow{ 
	
	protected HashMap<String, Object> connectParams;

	protected HashMap<String, Object> jobPage = new HashMap<String, Object>();
	
	protected String poolName;
	
	protected LinkedList<WriteUnit> datas = new LinkedList<WriteUnit>(); 
	
	protected AtomicBoolean isLocked = new AtomicBoolean(false); 
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName")); 
	}

	@Override
	public FnConnection<?> LINK(boolean canSharePipe) {
		return FnConnectionPool.getConn(this.connectParams,
				this.poolName,canSharePipe);
	}

	@Override
	public void UNLINK(FnConnection<?> FC,boolean releaseConn) {
		FnConnectionPool.freeConn(FC, this.poolName,releaseConn);
	}
	
	@Override
	public void MONOPOLY() { 
		
	} 

	public T getJobPage(HashMap<String, String> param,Map<String, WriteParam> writeParamMap,Handler handler) {
		return null;
	}

	public List<String> getPageSplit(HashMap<String, String> param) {
		return null;
	}
	
	public void freeJobPage() {
		this.isLocked.set(false);
		this.jobPage.clear(); 
		this.datas.clear();  
	} 
}
