package com.feiniu.task;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.instruction.flow.TransDataFlow;
import com.feiniu.node.CPU;
import com.feiniu.util.Common;

/**
 * schedule task description,to manage task job
 * @author chengwen 
 * @version 1.0 
 */
public class FlowTask{
	private boolean recompute = true;
	private String instanceName;
	private TransDataFlow transDataFlow;
	/**
	 * seq for scan series datas
	 */
	private String seq = ""; 
	/**
	 * runState,1 increment running 2 full running;
	 */
	private AtomicInteger runState = new AtomicInteger(0);
	
	private final static Logger log = LoggerFactory
			.getLogger(FlowTask.class);
	
	public static FlowTask createTask(String instanceName, TransDataFlow transDataFlow){
		return new FlowTask(instanceName, transDataFlow, GlobalParam.DEFAULT_RESOURCE_SEQ);
	}
	
	public static FlowTask createTask(String instanceName, TransDataFlow transDataFlow, String seq){
		return new FlowTask(instanceName, transDataFlow, seq);
	}
	
    private FlowTask(String instanceName, TransDataFlow transDataFlow, String seq) {
    	this.instanceName = instanceName;
    	this.transDataFlow = transDataFlow;
    	this.seq = seq;
    }   
	
	/**
	 * if no full job will auto open optimize job 
	 */
	public void optimizeInstance(){
		GlobalParam.FLOW_STATUS.get(instanceName,seq).set(0);
		String storeName = Common.getInstanceName(instanceName, seq, null,""); 
		CPU.RUN(transDataFlow.getID(), "Pond", "optimizeInstance", storeName, Common.getStoreId(instanceName,seq,transDataFlow,true,false)); 
		GlobalParam.FLOW_STATUS.get(instanceName,seq).set(1);
	}
	
	public void startFullJob() {
		if((this.runState.get()&2)==0){   
			this.runState.getAndAdd(2); 
			try{  
				Common.getStoreId(instanceName,seq,transDataFlow,true,false);
				String keepCurrentUpdateTime = GlobalParam.LAST_UPDATE_TIME.get(instanceName,seq);
					
				String storeId = Common.getStoreId(instanceName,seq,transDataFlow,false,false); 
				transDataFlow.run(instanceName, storeId, "-1", seq, true);
				GlobalParam.LAST_UPDATE_TIME.set(instanceName,seq,keepCurrentUpdateTime);
				GlobalParam.FLOW_STATUS.get(instanceName,seq).set(4); 
				Common.saveTaskInfo(instanceName,seq,storeId); 
			}catch(Exception e){
				log.error(instanceName+" Full Exception",e);
			}finally{
				this.runState.getAndAdd(-2);
				GlobalParam.FLOW_STATUS.get(instanceName,seq).set(1);
			}
		}else{
			log.info(instanceName+" full job is running, ignore this time job!");
		}
	} 
	
	
	public void startIncrementJob() {
		synchronized (this.runState) {
			if((this.runState.get()&1)==0){    
				if((GlobalParam.FLOW_STATUS.get(instanceName,seq).get()&1)>0){  
					this.runState.getAndAdd(1);
					GlobalParam.FLOW_STATUS.get(instanceName,seq).set(3);
					String storeId = Common.getStoreId(instanceName,seq,transDataFlow,true,recompute);
					String lastUpdateTime; 
					try {
						lastUpdateTime = transDataFlow.run(instanceName, storeId, GlobalParam.LAST_UPDATE_TIME.get(instanceName,seq), seq,false);
						GlobalParam.LAST_UPDATE_TIME.set(instanceName,seq, lastUpdateTime); 
					} catch (Exception e) { 
						if(e.getMessage().equals("storeId not found")){
							storeId = Common.getStoreId(instanceName,seq,transDataFlow,true,true);
							try {
								lastUpdateTime = transDataFlow.run(instanceName, storeId, GlobalParam.LAST_UPDATE_TIME.get(instanceName,seq), seq,false);
								GlobalParam.LAST_UPDATE_TIME.set(instanceName,seq, lastUpdateTime); 
							}catch (Exception ex) {
								log.error(instanceName+" Increment Exception",e);
							}
						}
						log.error(instanceName+" startIncrementJob Exception",e);
					}finally{
						recompute = false;
						GlobalParam.FLOW_STATUS.get(instanceName,seq).set(1);
						this.runState.getAndAdd(-1); 
					} 
				}else{
					log.info(instanceName+" job have been stopped!startIncrement JOB failed!");
				}
			}else{
				log.info(instanceName+" increment job is running, ignore this time job!");
			}
		}
	}  
	
}
