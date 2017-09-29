package com.feiniu.task;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.util.Common;
import com.feiniu.writer.flow.JobWriter;

/**
 * schedule task description,to manage task job
 * @author chengwen 
 * @version 1.0 
 */
public class Task{
	private boolean recompute = true;
	private String instanceName;
	private JobWriter jobWriter;
	/**
	 * seq for scan series datas
	 */
	private String seq = ""; 
	/**
	 * runState,1 increment running 2 full running;
	 */
	private AtomicInteger runState = new AtomicInteger(0);
	
	private final static Logger log = LoggerFactory
			.getLogger(Task.class);
	
	public static Task createTask(String instanceName, JobWriter writer){
		return new Task(instanceName, writer, "");
	}
	
	public static Task createTask(String instanceName, JobWriter writer, String seq){
		return new Task(instanceName, writer, seq);
	}
	
    private Task(String instanceName, JobWriter jobWriter, String seq) {
    	this.instanceName = instanceName;
    	this.jobWriter = jobWriter;
    	this.seq = seq;
    }  
    
	public void startFullIndex() {
		if((this.runState.get()&2)==0){  
			this.runState.set(this.runState.get()+2); 
			try{  
				Common.getStoreId(instanceName,seq,jobWriter,true,false);
				String keepCurrentUpdateTime = GlobalParam.LAST_UPDATE_TIME.get(instanceName);
					
				String storeId = Common.getStoreId(instanceName,seq,jobWriter,false,false); 
				jobWriter.write(instanceName, storeId, "-1", seq, true);
				GlobalParam.LAST_UPDATE_TIME.put(instanceName, keepCurrentUpdateTime);
				GlobalParam.FLOW_STATUS.get(instanceName).set(4); 
				Common.saveTaskInfo(instanceName,seq,storeId);
				this.runState.set(this.runState.get()-2);
				GlobalParam.FLOW_STATUS.get(instanceName).set(1);
			}catch(Exception e){
				log.error(instanceName+" Full Exception",e);
			}  
		}else{
			log.info(instanceName+" full job is running, ignore this time job!");
		}
	} 
	
	/**
	 * if no full job will auto open optimize job 
	 */
	public void optimizeIndex(){
		GlobalParam.FLOW_STATUS.get(instanceName).set(0);
		String indexName = instanceName;
		if (seq != null && seq.length() > 0)
			indexName = instanceName + seq;  
		jobWriter.optimizeIndex(indexName, Common.getStoreId(instanceName,seq,jobWriter,true,false));
		GlobalParam.FLOW_STATUS.get(instanceName).set(1);
	}
	
	public void startIncrementIndex() {
		if((this.runState.get()&1)==0){    
			if((GlobalParam.FLOW_STATUS.get(instanceName).get()&1)>0){  
				this.runState.set(this.runState.get()+1);
				GlobalParam.FLOW_STATUS.get(instanceName).set(3);
				String storeId = Common.getStoreId(instanceName,seq,jobWriter,true,recompute);
				String lastUpdateTime; 
				try {
					lastUpdateTime = jobWriter.write(instanceName, storeId, GlobalParam.LAST_UPDATE_TIME.get(instanceName), seq,false);
					GlobalParam.LAST_UPDATE_TIME.put(instanceName, lastUpdateTime); 
				} catch (Exception e) { 
					storeId = Common.getStoreId(instanceName,seq,jobWriter,true,true);
					try {
						lastUpdateTime = jobWriter.write(instanceName, storeId, GlobalParam.LAST_UPDATE_TIME.get(instanceName), seq,false);
						GlobalParam.LAST_UPDATE_TIME.put(instanceName, lastUpdateTime); 
					}catch (Exception ex) {
						log.error(instanceName+" Increment Exception",e);
					}
				}finally{
					recompute = false;
					GlobalParam.FLOW_STATUS.get(instanceName).set(1);
					this.runState.set(this.runState.get()-1);
				} 
			}else{
				log.info(instanceName+" job have been stopped!startIncrementIndex failed!");
			}
		}else{
			log.info(instanceName+" increment job is running, ignore this time job!");
		}
	} 
}
