package com.feiniu.writer.flow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.flow.Flow;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;

public class WriterFlowSocket implements Flow{
	
	/**batch submit documents*/
	protected Boolean batch = true;
	protected HashMap<String, Object> connectParams;
	protected String poolName;  
	protected FnConnection<?> FC;
	protected AtomicBoolean locked = new AtomicBoolean(false);
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.batch = GlobalParam.WRITE_BATCH;
	}

	@Override
	public FnConnection<?> PULL(boolean canSharePipe) {  
		this.FC = FnConnectionPool.getConn(this.connectParams,
				this.poolName,canSharePipe);
		return this.FC;
	}
	
	@Override
	public void CLOSED(FnConnection<?> FC) { 
		FnConnectionPool.freeConn(FC, this.poolName);
	}
	
	public void getResource(){}
	
	public void freeResource(){
		CLOSED(this.FC); 
		locked.set(false);
	}
	
	public boolean settings(String instantcName, String batchId, Map<String,WriteParam> paramMap) {
		return false;
	}
	
	public String getNewStoreId(String instanceName,boolean isIncrement,String dbseq, NodeConfig nodeConfig) {
		return null;
	}

	public void write(WriteUnit unit,Map<String, WriteParam> writeParamMap,String instantcName, String batchId,boolean isUpdate) throws Exception {
	}

	public void doDelete(WriteUnit unit, String instantcName, String batchId) throws Exception {
	}
  
	public void remove(String instanceName, String batchId) {
	}
	
	public void setAlias(String instanceName, String batchId, String aliasName) {
	}

	public void flush() throws Exception {
	}

	public void optimize(String instantcName, String batchId) {
	} 
}
