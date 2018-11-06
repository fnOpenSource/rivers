package com.feiniu.writer;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.field.RiverField;
import com.feiniu.flow.Flow;
import com.feiniu.model.reader.PipeDataUnit;

/**
 * Flow into Pond Manage
 * create with a/b switch mechanism
 * @author chengwen
 * @version 2.0
 */
@NotThreadSafe
public abstract class WriterFlowSocket extends Flow{
	
	/**batch submit documents*/
	protected Boolean isBatch = true;   
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.isBatch = GlobalParam.WRITE_BATCH; 
	}   
	
	public boolean create(String instance, String batchId, Map<String,RiverField> transParams) {
		return false;
	}
	
	public String getNewStoreId(String mainName,boolean isIncrement,InstanceConfig instanceConfig) {
		return null;
	}

	public void write(String keyColumn,PipeDataUnit unit,Map<String, RiverField> transParams,String instance, String batchId,boolean isUpdate) throws Exception {
	}

	public void delete(String instance, String storeId,String keyColumn,String keyVal) throws Exception {
	}
  
	public void removeInstance(String instance, String storeId) {
	}
	
	public void setAlias(String instance, String storeId, String aliasName) {
	}

	public void flush() throws Exception {
	}

	public void optimize(String instance, String storeId) {
	}  
}
