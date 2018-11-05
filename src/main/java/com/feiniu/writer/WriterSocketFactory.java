package com.feiniu.writer;

import java.lang.reflect.Method;
import java.util.HashMap;

import com.feiniu.flow.Socket;
import com.feiniu.param.warehouse.WarehouseNosqlParam;
import com.feiniu.param.warehouse.WarehouseParam;
import com.feiniu.param.warehouse.WarehouseSqlParam;
import com.feiniu.util.Common;
import com.feiniu.writer.flow.ESFlow;
import com.feiniu.writer.flow.HBaseFlow;
import com.feiniu.writer.flow.MysqlFlow;
import com.feiniu.writer.flow.SolrFlow;

/**
 * @param args getInstance function parameters:WarehouseParam param, String seq,String handler
 * @author chengwen
 *  @version 1.0
 */
public class WriterSocketFactory implements Socket<WriterFlowSocket>{ 
	
	private static WriterSocketFactory o = new WriterSocketFactory();
	
	public static WriterFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	}
	
 
	@Override
	public WriterFlowSocket getSocket(Object... args) {
		WarehouseParam param = (WarehouseParam) args[0];
		String seq = (String) args[1];
		String handler = (String) args[2];
		if(param instanceof WarehouseNosqlParam) {
			return getNoSqlFlow(param,seq,handler);
		}else {
			return getSqlFlow(param,seq,handler);
		} 
	} 
 
	
	private static WriterFlowSocket getSqlFlow(WarehouseParam param,String seq,String handler) {
		WriterFlowSocket writer = null;  
		WarehouseSqlParam params = (WarehouseSqlParam) param;
		HashMap<String, Object> connectParams = params.getConnectParams(seq);
		switch (params.getType()) {
		case MYSQL: 
			writer = MysqlFlow.getInstance(connectParams);
			break; 
		default:
			break;
		}
		return writer;
	}
	
	private static WriterFlowSocket getNoSqlFlow(WarehouseParam param,String seq,String handler) {
		WriterFlowSocket writer = null;  
		WarehouseNosqlParam params = (WarehouseNosqlParam) param;
		HashMap<String, Object> connectParams = params.getConnectParams(seq);
		if(handler!=null) {
			try {
				Class<?> clz = Class.forName("com.feiniu.writer.handler."+handler);
				Method m = clz.getMethod("getInstance",HashMap.class);
				writer = (WriterFlowSocket) m.invoke(null,connectParams);
			}catch (Exception e) {
				Common.LOG.error("getNoSqlFlow Exception!",e);
			} 
			return writer;
		}
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
				Common.LOG.error("WriterFlowSocket getWriter Type Not Support!");
				break; 
		} 
		return writer;
	} 
}
