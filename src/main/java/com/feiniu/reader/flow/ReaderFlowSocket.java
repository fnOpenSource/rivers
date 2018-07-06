package com.feiniu.reader.flow;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.flow.Flow;
import com.feiniu.model.PipeDataUnit;
import com.feiniu.model.param.TransParam;
import com.feiniu.reader.handler.Handler;

public class ReaderFlowSocket<T> implements Flow{ 
	
	protected HashMap<String, Object> connectParams;

	protected HashMap<String, Object> jobPage = new HashMap<String, Object>();
	
	protected String poolName;
	
	protected LinkedList<PipeDataUnit> datas = new LinkedList<PipeDataUnit>(); 
	
	protected AtomicBoolean isLocked = new AtomicBoolean(false); 
	
	protected FnConnection<?> FC;
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName")); 
	}

	@Override
	public FnConnection<?> GETSOCKET(boolean canSharePipe) {
		this.FC = FnConnectionPool.getConn(this.connectParams,
				this.poolName,canSharePipe);
		return this.FC;
	}

	@Override
	public boolean LINK(){
		if(this.FC==null) 
			return false;
		return true;
	} 
	
	@Override
	public boolean MONOPOLY() {
		return false;  
	} 
	
	@Override
	public void REALEASE(FnConnection<?> FC,boolean releaseConn) {
		FnConnectionPool.freeConn(FC, this.poolName,releaseConn);
	}

	public T getJobPage(HashMap<String, String> param,Map<String, TransParam> transParams,Handler handler) {
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
