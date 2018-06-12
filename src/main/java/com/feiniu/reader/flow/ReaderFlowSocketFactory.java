package com.feiniu.reader.flow;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.model.param.WarehouseSqlParam;
 
/**
 * 
 * @author chengwen
 *
 */
public class ReaderFlowSocketFactory {
	
	private final static Logger log = LoggerFactory
			.getLogger(ReaderFlowSocketFactory.class);
	 
	public static ReaderFlowSocket<?> getChannel(final WarehouseParam wParam, String seq){ 
		if (wParam.getType() == DATA_TYPE.MYSQL || wParam.getType() == DATA_TYPE.ORACLE){
			return sqlChannel((WarehouseSqlParam) wParam,seq);
		}else if(wParam.getType() == DATA_TYPE.HBASE){
			return noSqlChannel((WarehouseNosqlParam) wParam,seq);
		}else{
			log.error("Not support connection of "+wParam.getType());
			return null;
		}
	}
	static ReaderFlowSocket<?> sqlChannel(final WarehouseSqlParam wParam, String seq){ 
		HashMap<String, Object> connectParams = wParam.getConnectParams(seq);
		if (wParam.getType() == DATA_TYPE.MYSQL){ 
			return MysqlFlow.getInstance(connectParams);
		}else if((wParam.getType() == DATA_TYPE.ORACLE)){ 
			connectParams.put("sid", "CORD"); 
			return OracleFlow.getInstance(connectParams);
		}
		return null;
	}
	
	static ReaderFlowSocket<?> noSqlChannel(WarehouseNosqlParam wParam, String seq){
		HashMap<String, Object> connectParams = wParam.getConnectParams(seq); 
		if (wParam.getType() == DATA_TYPE.HBASE){ 
			return HbaseFlow.getInstance(connectParams);
		} 
		return null;
	}
}
