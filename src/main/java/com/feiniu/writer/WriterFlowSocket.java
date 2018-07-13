package com.feiniu.writer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.flow.Flow;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.PipeDataUnit;
import com.feiniu.model.param.TransParam;

/**
 * Flow into Pond Manage
 * create with a/b switch mechanism
 * @author chengwen
 * @version 1.0
 */
@NotThreadSafe
public class WriterFlowSocket implements Flow{
	
	/**batch submit documents*/
	protected volatile Boolean isBatch = true;
	protected HashMap<String, Object> connectParams;
	protected volatile String poolName;  
	protected FnConnection<?> FC;
	protected AtomicInteger retainer = new AtomicInteger(0);
	private final static Logger log = LoggerFactory.getLogger(WriterFlowSocket.class);
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.isBatch = GlobalParam.WRITE_BATCH;
		retainer.set(0);
	}

	@Override
	public FnConnection<?> GETSOCKET(boolean canSharePipe) {  
		this.FC = FnConnectionPool.getConn(this.connectParams,
				this.poolName,canSharePipe);
		return this.FC;
	}
	
	@Override
	public void REALEASE(FnConnection<?> FC,boolean releaseConn) { 
		FnConnectionPool.freeConn(FC, this.poolName,releaseConn);
	}
	
	@Override
	public boolean MONOPOLY() {
		if(this.FC==null) 
			return false;
		return true;
	} 
	
	@Override
	public boolean LINK(){
		if(this.FC==null) 
			return false;
		return true;
	}
	
	public void REALEASE(boolean releaseConn){
		synchronized(retainer){
			retainer.addAndGet(-1);
			if(retainer.get()==0){
				REALEASE(this.FC,releaseConn);   
			}else{
				log.info(this.FC+" retainer is "+retainer.get());
			}
		} 
	}
	
	public void freeConnPool() {
		FnConnectionPool.release(this.poolName);
	}
	
	public boolean create(String instantcName, String batchId, Map<String,TransParam> transParams) {
		return false;
	}
	
	public String getNewStoreId(String instanceName,boolean isIncrement,String dbseq, InstanceConfig instanceConfig) {
		return null;
	}

	public void write(String keyColumn,PipeDataUnit unit,Map<String, TransParam> transParams,String instantcName, String batchId,boolean isUpdate) throws Exception {
	}

	public void delete(SearcherModel<?, ?, ?> query, String instance, String storeId) throws Exception {
	}
  
	public void removeInstance(String instanceName, String batchId) {
	}
	
	public void setAlias(String instanceName, String batchId, String aliasName) {
	}

	public void flush() throws Exception {
	}

	public void optimize(String instantcName, String batchId) {
	}  
}
