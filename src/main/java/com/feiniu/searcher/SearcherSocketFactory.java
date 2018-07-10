package com.feiniu.searcher;

import java.util.HashMap;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.flow.Socket;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.searcher.flow.ESFlow;
import com.feiniu.searcher.flow.MysqlFlow;
import com.feiniu.searcher.flow.SolrFlow;

public class SearcherSocketFactory implements Socket<SearcherFlowSocket>{

	private static SearcherSocketFactory o = new SearcherSocketFactory();
	
	public static SearcherFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	} 
	 
	/** 
	 * @param args final WarehouseParam param, final InstanceConfig instanceConfig,
			String seq
	 * @return
	 */
	@Override
	public SearcherFlowSocket getSocket(Object... args) {
		WarehouseParam param = (WarehouseParam) args[0];
		InstanceConfig instanceConfig = (InstanceConfig) args[1];
		String seq = (String) args[2];
		if (param instanceof WarehouseNosqlParam) {
			return getNosqlFlowSocket(param, instanceConfig, seq);
		} else {
			return getSqlFlowSocket(param, instanceConfig, seq);
		}
	} 

	private static SearcherFlowSocket getNosqlFlowSocket(WarehouseParam params, InstanceConfig instanceConfig, String seq) {
		HashMap<String, Object> connectParams = params.getConnectParams(seq);
		connectParams.put("instanceConfig", instanceConfig);
		connectParams.put("handler", params.getHandler());
		connectParams.put("analyzer", GlobalParam.SEARCH_ANALYZER);
		SearcherFlowSocket searcher = null;
		switch (params.getType()) {
		case ES:
			searcher = ESFlow.getInstance(connectParams);
			break;
		case SOLR:
			searcher = SolrFlow.getInstance(connectParams);
			break;
		default:
			break;
		}
		return searcher;
	}

	private static SearcherFlowSocket getSqlFlowSocket(WarehouseParam params, InstanceConfig instanceConfig, String seq) {
		HashMap<String, Object> connectParams = params.getConnectParams(seq);
		SearcherFlowSocket searcher = null;
		switch (params.getType()) {
		case MYSQL:
			searcher = MysqlFlow.getInstance(connectParams);
			break;
		default:
			break;
		}
		return searcher;
	} 
}
