package com.feiniu.writer.flow;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.NodeConfig;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;

/**
 * HBase flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */
@NotThreadSafe
public class HBaseFlow extends WriterFlowSocket { 
	 
	private CopyOnWriteArrayList<Put> data = new CopyOnWriteArrayList<Put>();  
	private HTable conn;
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
		locked.set(false);
	} 
	
	@Override
	public void getResource(){
		while (locked.get()) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				log.error("getResource Exception", e);
			}
		}
		locked.set(true);
		PULL(false);
		this.conn = (HTable) this.FC.getConnection();
	} 
	 
	@Override
	public void write(WriteUnit unit,Map<String, WriteParam> writeParamMap, String instantcName, String storeId,boolean isUpdate) throws Exception { 
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
			put.add(Bytes.toBytes((String)connectParams.get("columnFamily")), Bytes.toBytes(indexParam.getName()),
					Bytes.toBytes(value)); 	
		} 
		data.add(put);
	}

	@Override
	public void doDelete(WriteUnit unit, String instantcName, String batchId) throws Exception {
		
	}

	@Override
	public void remove(String instanceName, String batchId) {
		
	}

	@Override
	public void setAlias(String instanceName, String batchId, String aliasName) {

	}

	@Override
	public void flush() throws Exception { 
		this.conn.put(data);
		data.clear();
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
