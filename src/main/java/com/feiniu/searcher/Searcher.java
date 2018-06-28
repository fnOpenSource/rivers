package com.feiniu.searcher;

import org.apache.lucene.analysis.Analyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.NodeConfig;
import com.feiniu.model.ESSearcherModel;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.FNRequest;
import com.feiniu.model.FNResponse;
import com.feiniu.model.SolrQueryModel;
import com.feiniu.searcher.flow.SearcherFlowSocket;
import com.feiniu.searcher.handler.Handler;
import com.feiniu.util.SearchParamUtil;

/**
 * provide search service
 * 
 * @author chengwen
 * @version 1.0
 */
public class Searcher {
	private final static Logger log = LoggerFactory.getLogger(Searcher.class);
	private SearcherFlowSocket searcher = null;
	private NodeConfig NodeConfig = null;
	private String instanceName = null;
	private Handler handler=null;

	public static Searcher getInstance(String instanceName,
			NodeConfig NodeConfig, SearcherFlowSocket searcher) {
		return new Searcher(instanceName, NodeConfig, searcher);
	}

	private Searcher(String instanceName, NodeConfig NodeConfig,
			SearcherFlowSocket searcher) {
		this.instanceName = instanceName;
		this.searcher = searcher;
		this.NodeConfig = NodeConfig;
		try {
			if(NodeConfig.getPipeParam().getSearcherHandler()!=null) {
				this.handler = (Handler) Class.forName(NodeConfig.getPipeParam().getSearcherHandler()).newInstance();
			}
		}catch(Exception e){
			log.error("FNSearcher Handler Exception",e);
		}
	}

	public FNResponse startSearch(FNRequest rq) {
		FNResponse response = FNResponse.getInstance();
		response.setIndex(instanceName);
		/** check validation */
		if (!rq.isValid()) {
			response.setError_info("handle is null!");
			return response;
		}

		if (this.searcher == null) {
			response.setError_info("searcher is null!");
			response.setParams(rq.getParams(), null);
			return response;
		}
		response.setParams(rq.getParams(), this.NodeConfig);
		Analyzer analyzer = this.searcher.getAnalyzer();

		SearcherModel<?, ?, ?> searcherModel = null;
		switch (this.searcher.getType()) {
		case ES:
			searcherModel = ESSearcherModel.getInstance(rq, analyzer,NodeConfig);
			SearchParamUtil.normalParam(rq, searcherModel,NodeConfig);
			break;
		case SOLR:
			searcherModel = SolrQueryModel.getInstance(rq, analyzer,NodeConfig);
			SearchParamUtil.normalParam(rq, searcherModel,NodeConfig);
			break; 
		default:
			response.setError_info("Not Support Searcher Type!");
			return response; 
		}  
		try {
			response.setError_info(rq.getErrors());
			response.setResult(this.searcher.Search(searcherModel, instanceName,handler));
		} catch (Exception e) {
			response.setError_info("search parameter may be error!");
			log.error("FNResponse error,", e);
		}
		return response;
	}  
} 