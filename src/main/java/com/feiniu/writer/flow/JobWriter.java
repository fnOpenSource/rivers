package com.feiniu.writer.flow;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.FNWriteResponse;
import com.feiniu.model.param.MessageParam;
import com.feiniu.model.param.NOSQLParam;
import com.feiniu.model.param.SQLParam;
import com.feiniu.model.param.WriteParam;
import com.feiniu.reader.flow.DataSetReader;
import com.feiniu.reader.flow.Reader;
import com.feiniu.reader.handler.Handler;
import com.feiniu.task.TaskManager;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.writer.jobFlow.WriteFlowSocket;

/**
 * write data from source A to Source B
 * @author chengwen
 * @version 1.0 
 */
public class JobWriter {

	private final static Logger log = LoggerFactory.getLogger(JobWriter.class); 
	private WriteFlowSocket<?> flowSocket; 
	private NodeConfig nodeConfig;
	private WriterFlowSocket writer;
	/**defined custom read flow socket*/
	private Handler handler;
	
	@Autowired
	private TaskManager TaskManager;

	private SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static JobWriter getInstance(WriteFlowSocket<?> flowSocket,WriterFlowSocket writer, NodeConfig nodeConfig){
		return new JobWriter(flowSocket,writer,nodeConfig);
	}
 
	private JobWriter(WriteFlowSocket<?> flowSocket,WriterFlowSocket writer, NodeConfig nodeConfig) {
		this.writer = writer;
		this.nodeConfig = nodeConfig;  
		this.flowSocket = flowSocket;
		try {
			if(nodeConfig.getTransParam().getHandler()!=null){
				this.handler = (Handler) Class.forName(nodeConfig.getTransParam().getHandler()).newInstance();
			} 
		}catch(Exception e){
			log.error("FNIndexer Construct Exception,",e);
		}
	}   
	
	public String write(String instanceName, String storeId, String lastTime,
			String DataSeq, boolean isFullIndex) throws FNException{
		if(this.nodeConfig.getTransParam().getNoSqlParam()!=null){
			return doNosqlWrite(instanceName, storeId, lastTime, isFullIndex);
		}else{
			return doSqlWrite(instanceName, storeId, lastTime, DataSeq, isFullIndex);
		}
	}  

	/**
	 * message scan from db write into index 
	 */
	public void scanDbWrite(String instanceName, String storeId,
			String dbseq, HashMap<String, String> params) throws Exception {
		MessageParam MP = nodeConfig.getMessageParam();
		String originalSql = MP.getSqlParam().getSql();
		if (null == originalSql) {
			log.error("scanDbWriteIndex No Message sql to deal!");
		} else {
			String indexName = instanceName;
			if (dbseq != null && dbseq.length() > 0) {
				indexName = instanceName + dbseq;
			}
			writeDataSet("Message",indexName, storeId, "",
					getSqlPageData(this.buildSql(originalSql, params),"",""), ",Message",false);
		}
	} 
	 

	public void optimizeIndex(String indexName, String storeId) {
		log.info("start optimize job "+indexName+"_"+storeId+" ...");
		this.writer.getResource();
		try{
			this.writer.optimize(indexName, storeId);
		}finally{
			this.writer.freeResource();
		}  
	} 

	public String getNewStoreId(String instanceName, boolean isIncrement,
			String dbseq) {
		this.writer.getResource();
		String taskId = null;
		try{
			taskId = this.writer.getNewStoreId(instanceName, isIncrement, dbseq,
					this.nodeConfig);
		}finally{
			this.writer.freeResource();
		}  
		return taskId;
	}
	
	public void createStorePosition(String store_main,String storeId){
		this.writer.getResource();
		try{
			this.writer.settings(store_main, storeId,
					getWriteParamMap());
		}finally{
			this.writer.freeResource();
		}  
	}
	
	public void switchSearcher(String instance, String storeId) {
		String removeId = ""; 
		if (storeId.equals("a")) {
			this.optimizeIndex(instance, "a");
			removeId = "b";
		} else {
			this.optimizeIndex(instance, "b");
			removeId = "a";
		}
		this.writer.getResource();
		try{
			this.writer.remove(instance, removeId);
			this.writer.setAlias(instance, storeId, nodeConfig.getAlias());
		}finally{
			this.writer.freeResource();
		}   
	}

	public Map<String, WriteParam> getWriteParamMap(){
		return nodeConfig.getWriteParamMap();
	}

/**
 * do index one page
 * @param id distinguish jobs,for write logs
 * @param instance Instance Name append with data source sequence,should use Common.getInstanceName to fetch
 * @param storeId
 * @param seq data source son level sequence
 * @param sql
 * @param info log info
 * @param incrementField for SQL scan job
 * @return FNWriteResponse 
 * @throws Exception
 */  
	public FNWriteResponse writeDataSet(String id,String instance, String storeId,
			String seq, HashMap<String, Object> dataSet, String info,boolean isUpdate) throws Exception {
		FNWriteResponse resp = new FNWriteResponse(); 
		Reader<HashMap<String, Object>> reader = new DataSetReader();
		reader.init(dataSet);
		long start = Common.getNow();
		int count = 0; 
		if (!reader.getLastUpdateTime().equals("-1")) {
			this.writer.getResource(); 
			try{
				while (reader.nextLine()) {   
					this.writer.write(reader.getLineData(),getWriteParamMap(),instance, storeId,isUpdate);
					count++;
				}
				String lastUpdateTime = reader.getLastUpdateTime();
				String maxId = reader.getMaxId();
				reader.close();
				resp.setLastUpdateTime(lastUpdateTime);
				resp.setMaxId(maxId);
				resp.setCount(count);
				indexLog(" -- "+id+" onepage ",instance, storeId, seq, String.valueOf(count), maxId,
						String.valueOf(lastUpdateTime), Common.getNow() - start, "onepage", info); 
			}finally{
				this.writer.flush();
				this.writer.freeResource();
			}  
		}
		flowSocket.freeJobPage();
		return resp;
	} 
	
	 
		/**
		 * write to not db platform
		 * @param indexName
		 * @param storeId
		 * @param lastTime
		 * @param isFullIndex
		 * @return
		 */
		@SuppressWarnings("unchecked")
		private String doNosqlWrite(String instanceName, String storeId, String lastTime,
				boolean isFullIndex)  throws FNException{  
			String desc = "increment";
			String indexName = Common.getInstanceName(instanceName,""); 
			if (isFullIndex) {
				createStorePosition(indexName, storeId); 
				desc = "full";
			} 
			NOSQLParam noSqlParam = nodeConfig.getTransParam().getNoSqlParam();
			try {
				if(lastTime==null || lastTime.equals(""))
					lastTime = "0";
				HashMap<String, String> param = new HashMap<String, String>();
				param.put("table", noSqlParam.getMainTable()); 
				param.put("column", noSqlParam.getKeyColumn());
				param.put("startTime", lastTime);  
				param.put(GlobalParam._incrementField, noSqlParam.getIncrementField());  
				List<String> pageList = flowSocket.getPageSplit(param); 
				if (pageList.size() > 0) {
					indexLog("start " + desc, indexName, storeId, "", "",
							"", lastTime, 0, "start", ",totalpage:"
									+ pageList.size());
					int processPos = 0;
					String startId ="";
					String endId = "";
					int total = 0;
					FNWriteResponse resp = null;
					long start = Common.getNow();
					for (String page : pageList) { 
						processPos++;
						endId = page;
						HashMap<String, String> pageParams = new HashMap<String, String>(); 
						pageParams.put(GlobalParam._start, startId);
						pageParams.put(GlobalParam._end, endId);
						pageParams.put(GlobalParam._start_time, lastTime);
						pageParams.put(GlobalParam._incrementField, noSqlParam.getIncrementField());
						 
						resp = writeDataSet(desc,indexName, storeId, "",
								(HashMap<String, Object>) flowSocket.getJobPage(pageParams,getWriteParamMap(),this.handler),
								",complete:" + processPos + "/" + pageList.size(),false);

						total += resp.getCount();
						startId = endId;
					}
					indexLog("complete " + desc, indexName, storeId, "",
							String.valueOf(total), "", lastTime,
							Common.getNow() - start, "complete", "");
				}
			}catch(Exception e){
				
			} 
			return lastTime;
		}

		/**
		 * database sql doIndex
		 *  
		 * @param storeId
		 * @param lastTime  get sql data filter with scan last timestamp
		 * @param lastBatchId
		 * @param DataSeq for series data source sequence
		 * @param instanceName data source main tag name
		 * @return String last update value
		 */
		private String doSqlWrite(String instanceName, String storeId, String lastTime,
				String DataSeq, boolean isFullIndex) throws FNException{
			String desc = "increment";
			String indexName = Common.getInstanceName(instanceName,DataSeq); 
			if (isFullIndex) {
				createStorePosition(indexName, storeId); 
				desc = "full";
			} 
			SQLParam sqlParam = nodeConfig.getTransParam().getSqlParam();
			List<String> table_seqs = sqlParam.getSeq().size() > 0 ? sqlParam.getSeq()
					: Arrays.asList("");
			String originalSql = sqlParam.getSql();
			String incrementField = sqlParam.getIncrementField();
			String keyColumn = sqlParam.getKeyColumn();

			String[] newLastUpdateTimes = new String[table_seqs.size()];
			String[] lastUpdateTimes = new String[table_seqs.size()];
			if (lastTime != null && lastTime.split(",").length == table_seqs.size()) {
				newLastUpdateTimes = lastTime.split(",");
				lastUpdateTimes = lastTime.split(",");
			}
			if(!GlobalParam.FLOW_INFOS.containsKey(instanceName+"_"+desc)){
				GlobalParam.FLOW_INFOS.put(instanceName+"_"+desc,new HashMap<String, String>());
			} 
			GlobalParam.FLOW_INFOS.get(instanceName+"_"+desc).put(indexName+" seqs nums",String.valueOf(table_seqs.size()));
			for (int i = 0; i < table_seqs.size(); i++) {
				int total = 0;
				FNWriteResponse resp = null;
				long start = Common.getNow();
				String lastUpdateTime = "0";
				String newLastUpdateTime = "0";
				String maxId = "0";
				String startId = "0";
				String tseq = table_seqs.get(i);

				if (null != lastUpdateTimes[i])
					lastUpdateTime = lastUpdateTimes[i];
				try {
					if(lastUpdateTime.equals("null"))
						lastUpdateTime = "0";
					HashMap<String, String> param = new HashMap<String, String>();
					param.put("table", sqlParam.getMainTable());
					param.put("alias", sqlParam.getMainAlias());
					param.put("column", sqlParam.getKeyColumn()); 
					param.put("startTime", lastUpdateTime);
					param.put("seq", tseq); 
					param.put("originalSql", originalSql);
					param.put("pageSql", sqlParam.getPageSql());
					param.put("keyColumnType", sqlParam.getKeyColumnType());
					List<String> pageList = flowSocket.getPageSplit(param); 
					HashMap<String, String> sqlParams;
					GlobalParam.FLOW_INFOS.get(instanceName+"_"+desc).put(indexName+tseq,"start count page...");
					if (pageList.size() > 0) {
						indexLog("start " + desc, indexName, storeId, tseq, "",
								maxId, lastUpdateTime, 0, "start", ",totalpage:"
										+ pageList.size()); 
						int processPos = 0;
						for (String page : pageList) { 
							processPos++;
							GlobalParam.FLOW_INFOS.get(instanceName+"_"+desc).put(indexName+tseq,processPos + "/" + pageList.size());
							maxId = page;
							sqlParams = null;
							sqlParams = new HashMap<String, String>();
							if (tseq != null && tseq.length() > 0)
								sqlParams.put(GlobalParam._seq, tseq);
							sqlParams.put(GlobalParam._start, startId);
							sqlParams.put(GlobalParam._end, maxId);
							sqlParams.put(GlobalParam._start_time, (lastUpdateTime.equals("null")?"0":lastUpdateTime));
							sqlParams.put(GlobalParam._incrementField, incrementField);
							String sql = buildSql(originalSql, sqlParams);
							resp = writeDataSet(desc,indexName, storeId, tseq,
									getSqlPageData(sql,incrementField,keyColumn),
									",complete:" + processPos + "/" + pageList.size(),false);

							total += resp.getCount();
							startId = maxId;
							
							if((GlobalParam.FLOW_STATUS.get(instanceName).get()&4)>0){
								indexLog("kill " + desc, indexName, storeId, tseq,
										String.valueOf(total), maxId, newLastUpdateTime,
										Common.getNow() - start, "complete", "");
								break;
							}
							
							if (newLastUpdateTimes[i]==null || newLastUpdateTimes[i].equals("null")	|| resp.getLastUpdateTime().compareTo(newLastUpdateTimes[i])>0) {
								newLastUpdateTimes[i] = String.valueOf(resp.getLastUpdateTime());
							}   
							if(!isFullIndex){ 
								GlobalParam.LAST_UPDATE_TIME.put(instanceName, getTimeString(newLastUpdateTimes));
								Common.saveTaskInfo(instanceName,DataSeq,storeId); 
							}
						}  
						indexLog("complete " + desc, indexName, storeId, tseq,
								String.valueOf(total), maxId, newLastUpdateTime,
								Common.getNow() - start, "complete", "");
					} else { 
						indexLog("start " + desc, indexName, storeId, tseq, "",
								maxId, lastUpdateTime, 0, "start",
								" no data job finished!");
					}
				} catch (Exception e) { 
					if (isFullIndex) {
						this.writer.getResource();
						try{
							this.writer.remove(indexName, storeId);
						}finally{
							this.writer.freeResource();
						} 
					} 
					if(e.getMessage().equals("storeId not found")){
						throw new FNException("storeId not found");
					}  
					log.error("[" + desc +" "+ indexName + tseq + "_" + storeId
							+ " ERROR]", e);
					GlobalParam.mailSender.sendHtmlMailBySynchronizationMode(
							" [SearchPlatForm] " + GlobalParam.run_environment,
							"Job " + indexName +" "+ desc + " Has stopped!");
					newLastUpdateTimes = lastUpdateTimes;
					TaskManager.startInstance(instanceName, nodeConfig, true);
					break;
				}  
			} 
			GlobalParam.FLOW_INFOS.get(instanceName+"_"+desc).clear();
			if (isFullIndex)
				switchSearcher(indexName, storeId);  
			
			return getTimeString(newLastUpdateTimes);
		} 
	
	/** 
	 * @param sql
	 * @param incrementField get auto incrementField to store
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private HashMap<String, Object> getSqlPageData(String sql,String incrementField,String keyColumn){
		HashMap<String, String> params = new HashMap<String, String>();
		params.put("sql", sql); 
		params.put("incrementField", incrementField); 
		params.put("keyColumn", keyColumn); 
		return (HashMap<String, Object>) flowSocket.getJobPage(params,getWriteParamMap(),this.handler);
	}
	
	private String getTimeString(String[] strs) {
		if (strs.length > 0) {
			StringBuffer sb = new StringBuffer();
			for (String s : strs) {
				sb.append(",");
				sb.append(s);
			}
			return sb.toString().substring(1);
		} else {
			return "0";
		}
	}
	
	/**
	 * 
	 * @param heads
	 * @param instanceName
	 * @param storeId
	 * @param seq table seq
	 * @param total
	 * @param maxId
	 * @param lastUpdateTime
	 * @param useTime
	 * @param types
	 * @param moreinfo
	 */
	
	private void indexLog(String heads,String instanceName, String storeId,
			String seq, String total, String maxId, String lastUpdateTime,
			long useTime, String types, String moreinfo) {
		String useTimeFormat = Common.seconds2time(useTime);
		String str = "["+heads+" "+instanceName + "_" + storeId+"] "+(!seq.equals("") ? " table:" + seq : "");
		String update;
		if(lastUpdateTime.length()>9){
			update = this.SDF.format(lastUpdateTime.length()<12?new Long(lastUpdateTime+"000"):new Long(lastUpdateTime));
		}else{
			update = lastUpdateTime;
		} 
		switch (types) {
		case "complete":
			str += " docs:" + total	+ " " + " useTime: " + useTimeFormat + "}";
			break;
		case "start": 
			str +=" lastUpdate:" + update;
			break;
		default:
			str +=" docs:" + total+ (maxId.equals("0") ? "" : " MaxId:" + maxId)
			+ " lastUpdate:" + update + " useTime:"	+ useTimeFormat;
			break;
		} 
		log.info(str + moreinfo);
	}

	/**
	 * build sql from params
	 * 
	 * @param sql
	 * @param seq
	 * @param startId
	 * @param maxId
	 * @param lastUpdateTime
	 * @param updateTime
	 * @return
	 */
	private String buildSql(String sql, HashMap<String, String> params) {
		String res = sql;
		Iterator<String> entries = params.keySet().iterator();
		while (entries.hasNext()) {
			String k = entries.next();
			if (k.indexOf("#{") > -1)
				res = res.replace(k, params.get(k));
		}
		return res;
	}
}
