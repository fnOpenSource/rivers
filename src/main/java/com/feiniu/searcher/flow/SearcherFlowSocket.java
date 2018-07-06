package com.feiniu.searcher.flow;

import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;

import com.feiniu.config.InstanceConfig;
import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.flow.Flow;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.SearcherResult;
import com.feiniu.searcher.handler.Handler;

public class SearcherFlowSocket implements Flow{
	
	protected Analyzer analyzer;
	protected InstanceConfig instanceConfig;
	protected HashMap<String, Object> connectParams;
	protected String poolName;
	protected FnConnection<?> FC;
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.instanceConfig = (InstanceConfig) this.connectParams.get("instanceConfig");
		this.analyzer = (Analyzer) this.connectParams.get("analyzer");
	} 
	
	public Analyzer getAnalyzer() {
		return this.analyzer;
	}
	/**need rewrite*/
	public SearcherResult Search(SearcherModel<?, ?, ?> query, String instance,Handler handler) throws Exception {
		return null;
	}
	
	public DATA_TYPE getType() {
		return (DATA_TYPE) connectParams.get("type");
	}
	
	@Override
	public FnConnection<?> GETSOCKET(boolean canSharePipe) {  
		this.FC = FnConnectionPool.getConn(this.connectParams,
				this.poolName,canSharePipe);
		return this.FC;
	}
	
	@Override
	public void REALEASE(FnConnection<?> FC,boolean releaseConn) { 
		FnConnectionPool.freeConn(FC, this.poolName,releaseConn);
	}

	@Override
	public boolean MONOPOLY() {
		if(this.FC==null) 
			return false;
		return true;
	}

	@Override
	public boolean LINK() { 
		if(this.FC==null) 
			return false;
		return true;
	} 
}
