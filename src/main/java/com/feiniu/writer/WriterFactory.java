package com.feiniu.writer;

import java.util.HashMap;

import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.writer.flow.ESFlow;
import com.feiniu.writer.flow.HBaseFlow;
import com.feiniu.writer.flow.SolrFlow;
import com.feiniu.writer.flow.WriterFlowSocket;

public class WriterFactory {
	
	public static WriterFlowSocket getWriter(final WarehouseNosqlParam param) {
		WriterFlowSocket writer = null; 
		HashMap<String, Object> connectParams = new HashMap<String, Object>(); 
		connectParams.put("alias", param.getAlias());
		connectParams.put("defaultValue", param.getDefaultValue()); 
		connectParams.put("ip", param.getIp());
		connectParams.put("name", param.getName()); 
		connectParams.put("type", param.getType()); 
		connectParams.put("poolName", param.getPoolName(null));
		if (param.getType() == DATA_TYPE.ES) {
			writer = ESFlow.getInstance(connectParams);
		} else if (param.getType() == DATA_TYPE.SOLR) {
			writer = SolrFlow.getInstance(connectParams);
		} else if (param.getType() == DATA_TYPE.HBASE) {
			writer = HBaseFlow.getInstance(connectParams);
		} 
		return writer;
	}
	
}
