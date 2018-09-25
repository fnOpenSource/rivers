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
	}
 
	@Override
	public FnConnection<?> PREPARE(boolean isMonopoly,boolean canSharePipe) {   
		if(isMonopoly) {
			synchronized (this) {
				if(this.FC==null) 
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
			} 
		}else {
			synchronized (retainer) { 
				if(retainer.incrementAndGet()==1 || this.FC==null) {
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
					retainer.set(1);;
				}
			} 
		} 
		return this.FC;
	}
	
	@Override
	public FnConnection<?> GETSOCKET() { 
		return this.FC;
	}

	@Override
	public boolean ISLINK() {  
		if(this.FC==null) 
			return false;
		return true;
	} 
	
	public void REALEASE(boolean isMonopoly,boolean releaseConn){
		if(isMonopoly==false) { 
			synchronized(retainer){ 
				if(retainer.decrementAndGet()<=0){
					FnConnectionPool.freeConn(this.FC, this.poolName,releaseConn);
					retainer.set(0); 
				}else{
					log.info(this.FC+" retainer is "+retainer.get());
				}
			} 
		} 
	}
	
	public void freeConnPool() {
		FnConnectionPool.release(this.poolName);
	}
	
	public boolean create(String instance, String batchId, Map<String,TransParam> transParams) {
		return false;
	}
	
	public String getNewStoreId(String mainName,boolean isIncrement,InstanceConfig instanceConfig) {
		return null;
	}

	public void write(String keyColumn,PipeDataUnit unit,Map<String, TransParam> transParams,String instance, String batchId,boolean isUpdate) throws Exception {
	}

	public void delete(String instance, String storeId,String keyColumn,String keyVal) throws Exception {
	}
  
	public void removeInstance(String instance, String batchId) {
	}
	
	public void setAlias(String instance, String batchId, String aliasName) {
	}

	public void flush() throws Exception {
	}

	public void optimize(String instance, String storeId) {
	}  
}
