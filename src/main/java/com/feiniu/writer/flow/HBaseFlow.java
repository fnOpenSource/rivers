package com.feiniu.writer.flow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.NodeConfig;
import com.feiniu.model.FNQuery;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;

/**
 * HBase flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */
@ThreadSafe
public class HBaseFlow extends WriterFlowSocket { 
	 
	private List<Put> data = new CopyOnWriteArrayList<Put>();  
	private Table conn;
	private final static Logger log = LoggerFactory.getLogger(HBaseFlow.class); 
	
	public static HBaseFlow getInstance(HashMap<String, Object> connectParams) {
		HBaseFlow o = new HBaseFlow();
		o.INIT(connectParams);
		return o;
	}
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;  
		String tableColumnFamily = (String) this.connectParams.get("defaultValue");
		if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
			String[] strs = tableColumnFamily.split(":");
			if (strs != null && strs.length > 0)
				this.connectParams.put("tableName", strs[0]);
			if (strs != null && strs.length > 1)
				this.connectParams.put("columnFamily", strs[1]);
		}
		this.poolName = String.valueOf(connectParams.get("poolName"));;
		retainer.set(0);
	} 
	
	@Override
	public void getResource(){
		synchronized(retainer){
			if(retainer.get()==0){
				LINK(false);
				this.conn = (Table) this.FC.getConnection(false);
			} 
			retainer.addAndGet(1); 
		} 
	} 
	  
	@Override
	public void write(String keyColumn,WriteUnit unit,Map<String, WriteParam> writeParamMap, String instantcName, String storeId,boolean isUpdate) throws Exception { 
		if (unit.getData().size() == 0){
			log.info("Empty IndexUnit for " + instantcName + " " + storeId);
			return;
		}  
		String id = unit.getKeyColumnVal(); 
		Put put = new Put(Bytes.toBytes(id));
		
		for(Entry<String, Object> r:unit.getData().entrySet()){
			String field = r.getKey(); 
			if (r.getValue() == null)
				continue;
			String value = String.valueOf(r.getValue());
			if (field.equalsIgnoreCase("update_time") && value!=null)
				value = String.valueOf(System.currentTimeMillis());
			
			if (value == null)
				continue;
			
			WriteParam indexParam = writeParamMap.get(field);
			if (indexParam == null)
				indexParam = writeParamMap.get(field.toLowerCase());
			if (indexParam == null)
				indexParam = writeParamMap.get(field.toUpperCase());
			if (indexParam == null)
				continue; 
			put.addColumn(Bytes.toBytes((String)connectParams.get("columnFamily")), Bytes.toBytes(indexParam.getName()),
					Bytes.toBytes(value));  
		} 
		synchronized (data) {
			data.add(put); 
		}
	} 

	@Override
	public void doDelete(FNQuery<?, ?, ?> query, String instance, String storeId) throws Exception {
		
	}

	@Override
	public void remove(String instanceName, String batchId) {
		
	}

	@Override
	public void setAlias(String instanceName, String batchId, String aliasName) {

	}

	@Override
	public void flush() throws Exception { 
		synchronized (data) {
			this.conn.put(data);
			data.clear();
		} 
	}

	@Override
	public void optimize(String instantcName, String batchId) {
		
	}
 
	@Override
	public boolean settings(String instantcName, String batchId, Map<String, WriteParam> paramMap) {
		return true;
	}

	@Override
	public String getNewStoreId(String instanceName,boolean isIncrement,String dbseq, NodeConfig nodeConfig) {
		// TODO Auto-generated method stub
		return "a";
	}
 
}
