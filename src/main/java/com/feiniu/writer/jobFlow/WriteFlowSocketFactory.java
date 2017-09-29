package com.feiniu.writer.jobFlow;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.model.param.WarehouseSqlParam;
 
public class WriteFlowSocketFactory {
	
	private final static Logger log = LoggerFactory
			.getLogger(WriteFlowSocketFactory.class);
	 
	public static WriteFlowSocket<?> getChannel(final WarehouseParam wParam, String seq){ 
		if (wParam.getType() == DATA_TYPE.MYSQL || wParam.getType() == DATA_TYPE.ORACLE){
			return sqlChannel((WarehouseSqlParam) wParam,seq);
		}else if(wParam.getType() == DATA_TYPE.HBASE){
			return noSqlChannel((WarehouseNosqlParam) wParam,seq);
		}else{
			log.error("Not support connection of "+wParam.getType());
			return null;
		}
	}
	static WriteFlowSocket<?> sqlChannel(final WarehouseSqlParam wParam, String seq){
		String dbname = (seq != null) ? wParam.getDbname().replace("#{seq}", seq) : wParam.getDbname();
		HashMap<String, Object> connectParams = new HashMap<String, Object>();
		connectParams.put("host", wParam.getHost());
		connectParams.put("port", String.valueOf(wParam.getPort()));
		connectParams.put("dbname", dbname);
		connectParams.put("user", wParam.getUser());
		connectParams.put("password", wParam.getPassword()); 
		connectParams.put("type", wParam.getType());
		connectParams.put("poolName", wParam.getPoolName(seq));
		if (wParam.getType() == DATA_TYPE.MYSQL){ 
			return MysqlFlow.getInstance(connectParams);
		}else if((wParam.getType() == DATA_TYPE.ORACLE)){ 
			connectParams.put("sid", "CORD"); 
			return OracleFlow.getInstance(connectParams);
		}
		return null;
	}
	
	static WriteFlowSocket<?> noSqlChannel(WarehouseNosqlParam wParam, String seq){
		HashMap<String, Object> connectParams = new HashMap<String, Object>();
		connectParams.put("alias", wParam.getAlias());
		connectParams.put("defaultValue", wParam.getDefaultValue()); 
		connectParams.put("ip", wParam.getIp());
		connectParams.put("name", wParam.getName()); 
		connectParams.put("type", wParam.getType()); 
		connectParams.put("poolName", wParam.getPoolName(null));
		if (wParam.getType() == DATA_TYPE.HBASE){ 
			return HbaseFlow.getInstance(connectParams);
		} 
		return null;
	}
}
