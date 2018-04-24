package com.feiniu.writer;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.writer.flow.ESFlow;
import com.feiniu.writer.flow.HBaseFlow;
import com.feiniu.writer.flow.SolrFlow;
import com.feiniu.writer.flow.WriterFlowSocket;

public class WriterFactory {
	
	private final static Logger log = LoggerFactory
			.getLogger(WriterFactory.class);
	
	public static WriterFlowSocket getWriter(final WarehouseNosqlParam param) {
		WriterFlowSocket writer = null; 
		HashMap<String, Object> connectParams = new HashMap<String, Object>(); 
		connectParams.put("alias", param.getAlias());
		connectParams.put("defaultValue", param.getDefaultValue()); 
		connectParams.put("ip", param.getIp());
		connectParams.put("name", param.getName()); 
		connectParams.put("type", param.getType()); 
		connectParams.put("poolName", param.getPoolName(null));
		switch (param.getType()) {
			case ES:
				writer = ESFlow.getInstance(connectParams);
				break;
			case SOLR:
				writer = SolrFlow.getInstance(connectParams);
				break;
			case HBASE:
				writer = HBaseFlow.getInstance(connectParams);
				break;
			default:
				log.error("WriterFlowSocket getWriter Type Not Support!");
				break; 
		} 
		return writer;
	}
}
