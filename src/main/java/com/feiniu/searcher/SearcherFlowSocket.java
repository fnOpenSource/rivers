package com.feiniu.searcher;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;

import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.config.InstanceConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.flow.Flow;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.SearcherResult;
import com.feiniu.searcher.handler.Handler;

/** 
 * @author chengwen
 * @version 1.0 
 */
public class SearcherFlowSocket implements Flow{
	
	protected Analyzer analyzer;
	protected InstanceConfig instanceConfig;
	protected volatile HashMap<String, Object> connectParams;
	protected String poolName;
	protected FnConnection<?> FC;
	protected AtomicInteger retainer = new AtomicInteger(0);
	
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
	public FnConnection<?> PREPARE(boolean isMonopoly,boolean canSharePipe) {  
		if(isMonopoly) {
			synchronized (this) {
				if(this.FC==null) 
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
			} 
		}else {
			synchronized (retainer) { 
				if(retainer.incrementAndGet()==1 || this.FC==null) {
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
					retainer.set(1);;
				}
			} 
		} 
		return this.FC;
	}
	
	@Override
	public void REALEASE(boolean isMonopoly,boolean releaseConn) { 
		if(isMonopoly==false) { 
			synchronized(retainer){ 
				if(retainer.decrementAndGet()<=0){
					FnConnectionPool.freeConn(this.FC, this.poolName,releaseConn);
					retainer.set(0);
				}
			} 
		}
	} 
 
	@Override
	public FnConnection<?> GETSOCKET() { 
		return this.FC;
	}

	@Override
	public boolean ISLINK() { 
		if(this.FC==null) 
			return false;
		return true;
	} 
}
