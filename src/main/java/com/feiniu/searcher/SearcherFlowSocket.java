package com.feiniu.searcher;

import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;

import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.config.InstanceConfig;
import com.feiniu.flow.Flow;
import com.feiniu.model.searcher.SearcherModel;
import com.feiniu.model.searcher.SearcherResult;
import com.feiniu.searcher.handler.Handler;

/** 
 * @author chengwen
 * @version 2.0 
 */
public abstract class SearcherFlowSocket extends Flow{
	
	protected Analyzer analyzer;
	protected InstanceConfig instanceConfig;  
	
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
 
}
