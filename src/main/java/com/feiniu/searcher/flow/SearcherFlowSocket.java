package com.feiniu.searcher.flow;

import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;

import com.feiniu.config.NodeConfig;
import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.flow.Flow;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.FNResultSet;
import com.feiniu.searcher.handler.Handler;

public class SearcherFlowSocket implements Flow{
	
	protected Analyzer analyzer;
	protected NodeConfig NodeConfig;
	protected HashMap<String, Object> connectParams;
	protected String poolName;
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.NodeConfig = (NodeConfig) this.connectParams.get("nodeConfig");
		this.analyzer = (Analyzer) this.connectParams.get("analyzer");
	} 
	
	public Analyzer getAnalyzer() {
		return this.analyzer;
	}
	/**need rewrite*/
	public FNResultSet Search(SearcherModel<?, ?, ?> query, String instance,Handler handler) throws Exception {
		return null;
	}
	
	public DATA_TYPE getType() {
		return (DATA_TYPE) connectParams.get("type");
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
}
