package com.feiniu.searcher;

import java.util.HashMap;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.searcher.flow.ESFlow;
import com.feiniu.searcher.flow.SearcherFlowSocket;
import com.feiniu.searcher.flow.SolrFlow;

public class FNSearcherSocketFactory {
	
	public static SearcherFlowSocket getSearcherInstance(final WarehouseParam param, final NodeConfig NodeConfig)
	{
		SearcherFlowSocket searcher = null;
		HashMap<String, Object> connectParams = new HashMap<String, Object>();
		connectParams.put("nodeConfig", NodeConfig); 
		connectParams.put("handler", param.getHandler());
		connectParams.put("poolName", param.getPoolName(null));
		connectParams.put("analyzer", GlobalParam.SEARCH_ANALYZER);
		if (param.getType() == DATA_TYPE.ES){
			WarehouseNosqlParam pm = (WarehouseNosqlParam) param;
			connectParams.put("ip", pm.getIp());
			connectParams.put("defaultValue", pm.getDefaultValue());
			connectParams.put("name", pm.getName());
			connectParams.put("type", pm.getType()); 
			searcher = ESFlow.getInstance(connectParams); 
		}
		else if (param.getType() == DATA_TYPE.SOLR){
			WarehouseNosqlParam pm = (WarehouseNosqlParam) param;
			connectParams.put("ip", pm.getIp());
			connectParams.put("defaultValue", pm.getDefaultValue());
			connectParams.put("name", pm.getName());
			connectParams.put("type", pm.getType()); 
			searcher = SolrFlow.getInstance(connectParams);		
		} 
		return searcher;
	}
}
