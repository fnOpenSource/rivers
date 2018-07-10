package com.feiniu.searcher;

import org.apache.lucene.analysis.Analyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.InstanceConfig;
import com.feiniu.model.SearcherESModel;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.SearcherRequest;
import com.feiniu.model.SearcherState;
import com.feiniu.model.SearcherSolrModel;
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
	private InstanceConfig instanceConfig = null;
	private String instanceName = null;
	private Handler handler=null;

	public static Searcher getInstance(String instanceName,
			InstanceConfig instanceConfig, SearcherFlowSocket searcher) {
		return new Searcher(instanceName, instanceConfig, searcher);
	}

	private Searcher(String instanceName, InstanceConfig instanceConfig,
			SearcherFlowSocket searcher) {
		this.instanceName = instanceName;
		this.searcher = searcher;
		this.instanceConfig = instanceConfig;
		try {
			if(instanceConfig.getPipeParam().getSearcherHandler()!=null) {
				this.handler = (Handler) Class.forName(instanceConfig.getPipeParam().getSearcherHandler()).newInstance();
			}
		}catch(Exception e){
			log.error("FNSearcher Handler Exception",e);
		}
	}

	public SearcherState startSearch(SearcherRequest rq) {
		SearcherState response = SearcherState.getInstance();
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
		response.setParams(rq.getParams(), instanceConfig);
		Analyzer analyzer = this.searcher.getAnalyzer();

		SearcherModel<?, ?, ?> searcherModel = null;
		switch (this.searcher.getType()) {
		case ES:
			searcherModel = SearcherESModel.getInstance(rq, analyzer,instanceConfig);
			SearchParamUtil.normalParam(rq, searcherModel,instanceConfig);
			break;
		case SOLR:
			searcherModel = SearcherSolrModel.getInstance(rq, analyzer,instanceConfig);
			SearchParamUtil.normalParam(rq, searcherModel,instanceConfig);
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