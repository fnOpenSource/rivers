package com.feiniu.writer;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.model.param.WarehouseSqlParam;
import com.feiniu.writer.flow.ESFlow;
import com.feiniu.writer.flow.HBaseFlow; 
import com.feiniu.writer.flow.SolrFlow;
import com.feiniu.writer.flow.WriterFlowSocket;

/**
 * 
 * @author chengwen
 *
 */
public class WriterFactory {
	
	private final static Logger log = LoggerFactory
			.getLogger(WriterFactory.class);
	
	public static WriterFlowSocket getWriter(final WarehouseParam param, String seq) {
		if(param instanceof WarehouseNosqlParam) {
			return getNoSqlFlow(param,seq);
		}else {
			return getSqlFlow(param,seq);
		} 
	}
	
	private static WriterFlowSocket getSqlFlow(WarehouseParam param,String seq) {
		WriterFlowSocket writer = null;  
		WarehouseSqlParam params = (WarehouseSqlParam) param;
		//HashMap<String, Object> connectParams = param.getConnectParams(seq);
		switch (params.getType()) {
		case MYSQL:  
			break; 
		default:
			break;
		}
		return writer;
	}
	
	private static WriterFlowSocket getNoSqlFlow(WarehouseParam param,String seq) {
		WriterFlowSocket writer = null;  
		WarehouseNosqlParam params = (WarehouseNosqlParam) param;
		HashMap<String, Object> connectParams = params.getConnectParams(seq);
		switch (params.getType()) {
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
