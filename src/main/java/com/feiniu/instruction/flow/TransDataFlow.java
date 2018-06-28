package com.feiniu.instruction.flow;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.config.GlobalParam.JOB_TYPE;
import com.feiniu.correspond.ReportStatus;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.ReaderState;
import com.feiniu.model.param.MessageParam;
import com.feiniu.model.param.NOSQLParam;
import com.feiniu.model.param.SQLParam;
import com.feiniu.reader.Reader;
import com.feiniu.reader.flow.ReaderFlowSocket;
import com.feiniu.reader.handler.Handler;
import com.feiniu.reader.util.DataSetReader;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.writer.flow.WriterFlowSocket;

/**
 * write data from source A to Source B
 * @author chengwen
 * @version 1.0 
 */
public final class TransDataFlow {

	private final static Logger log = LoggerFactory.getLogger("TransDataFlow"); 
	private ReaderFlowSocket<?> reader; 
	private NodeConfig nodeConfig;
	private WriterFlowSocket writer;
	/**defined custom read flow socket*/
	private Handler readHandler; 
	private Handler scanHandler; 

	private SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static TransDataFlow getInstance(ReaderFlowSocket<?> flowSocket,WriterFlowSocket writer, NodeConfig nodeConfig){
		return new TransDataFlow(flowSocket,writer,nodeConfig);
	}
 
	private TransDataFlow(ReaderFlowSocket<?> reader,WriterFlowSocket writer, NodeConfig nodeConfig) {
		this.writer = writer;
		this.nodeConfig = nodeConfig;  
		this.reader = reader;
		try {
			if(nodeConfig.getPipeParam().getDataFromhandler()!=null){
				this.readHandler = (Handler) Class.forName(nodeConfig.getPipeParam().getDataFromhandler()).newInstance();
			} 
			if(nodeConfig.getPipeParam().getSqlParam().getHandler()!=null) {
				this.scanHandler = (Handler) Class.forName(nodeConfig.getPipeParam().getSqlParam().getHandler()).newInstance();
			}
		}catch(Exception e){
			log.error("TransDataFlow Instruction Exception,",e);
		}
	}   
	
	public String write(String instanceName, String storeId, String lastTime,
			String DataSeq, boolean isFullIndex) throws FNException{
		if(this.nodeConfig.getPipeParam().getNoSqlParam()!=null){
			return doNosqlWrite(instanceName, storeId, lastTime, isFullIndex);
		}else{
			return doSqlWrite(instanceName, storeId, lastTime, DataSeq, isFullIndex);
		}
	}  
	
	public NodeConfig getNodeConfig() {
		return nodeConfig;
	}

	/**
	 * message scan from db write into index 
	 */
	public void scanDbWrite(String instanceName, String storeId,
			String dbseq, HashMap<String, String> params) throws Exception {
		MessageParam MP = nodeConfig.getMessageParam();
		String originalSql = MP.getSqlParam().getSql();
		if (null == originalSql) {
			log.error("scanData with null sql!");
		} else {
			String indexName = instanceName;
			if (dbseq != null && dbseq.length() > 0) {
				indexName = instanceName + dbseq;
			}
			writeDataSet("Message",indexName, storeId, "",
					getSqlPageData(buildSql(originalSql, params),"",""), ",Message",false,false);
		}
	} 
	 

	public void optimizeIndex(String instance, String storeId) {
		log.info("start optimize instance "+instance+"_"+storeId+" ...");
		this.writer.getResource();
		try{
			this.writer.optimize(instance, storeId);
		}finally{
			this.writer.freeResource(false);
		}  
	} 

	public String getNewStoreId(String instanceName, boolean isIncrement,
			String dbseq) {
		String taskId = null;
		this.writer.getResource(); 
		try{
			taskId = this.writer.getNewStoreId(instanceName, isIncrement, dbseq,
					this.nodeConfig);
		}finally{
			this.writer.freeResource(false);
		}  
		return taskId;
	}
	
	public void createStorePosition(String store_main,String storeId){
		this.writer.getResource();
		try{
			this.writer.settings(store_main, storeId,nodeConfig.getTransParams());
		}finally{
			this.writer.freeResource(false);
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
			this.writer.freeResource(true); 
			this.writer.freeConnPool();
		}   
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
 * @param isUpdate write with update method
 * @param monopoly monopoly resource not release
 * @return FNWriteResponse 
 * @throws Exception
 */  
	public ReaderState writeDataSet(String id,String instance, String storeId,
			String seq, HashMap<String, Object> dataSet, String info,boolean isUpdate,boolean monopoly) throws Exception {
		ReaderState resp = new ReaderState(); 
		Reader<HashMap<String, Object>> scaner = new DataSetReader();
		scaner.init(dataSet);
		long start = Common.getNow();
		int num = 0; 
		if (scaner.status()) {
			if(monopoly) {
				this.writer.MONOPOLY();
			}else {
				this.writer.getResource();
			} 
			boolean freeConn = false;
			try{
				while (scaner.nextLine()) {   
					this.writer.write(scaner.getkeyColumn(),scaner.getLineData(),nodeConfig.getTransParams(),instance, storeId,isUpdate);
					num++;
				}
				String READER_LAST_STAMP = scaner.getScanStamp();
				String maxId = scaner.getMaxId();
				scaner.close();
				resp.setReaderScanStamp(READER_LAST_STAMP);
				resp.setMaxId(maxId);
				resp.setCount(num);
				indexLog(" -- "+id+" onepage ",instance, storeId, seq, String.valueOf(num), maxId,
						String.valueOf(READER_LAST_STAMP), Common.getNow() - start, "onepage", info); 
			}catch(Exception e){
				if(e.getMessage().equals("storeId not found")){
					throw new FNException("storeId not found");
				}else{
					freeConn = true;
				}
			}finally{
				this.writer.flush();
				if(!monopoly) 
					this.writer.freeResource(freeConn);
			}  
		}
		reader.freeJobPage();
		return resp;
	}  
	
		public void deleteByQuery(SearcherModel<?, ?, ?> query,String instance, String storeId) {
			this.writer.getResource(); 
			boolean freeConn = false;
			try{
				this.writer.doDelete(query, instance, storeId);
			}catch(Exception e){
				log.error("DeleteByQuery Exception",e);
				freeConn = true;
			}finally{
				this.writer.freeResource(freeConn);
			}
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
			String indexName = Common.getInstanceName(instanceName,"",nodeConfig.getPipeParam().getInstanceName()); 
			if (isFullIndex) {
				createStorePosition(indexName, storeId); 
				desc = "full";
			} 
			NOSQLParam noSqlParam = nodeConfig.getPipeParam().getNoSqlParam();
			try {
				if(lastTime==null || lastTime.equals(""))
					lastTime = "0";
				HashMap<String, String> param = new HashMap<String, String>();
				param.put("table", noSqlParam.getMainTable()); 
				param.put("column", noSqlParam.getKeyColumn());
				param.put("startTime", lastTime);  
				param.put(GlobalParam._incrementField, noSqlParam.getIncrementField());  
				List<String> pageList = reader.getPageSplit(param); 
				if (pageList.size() > 0) {
					indexLog("start " + desc, indexName, storeId, "", "",
							"", lastTime, 0, "start", ",totalpage:"
									+ pageList.size());
					int processPos = 0;
					String startId ="";
					String endId = "";
					int total = 0;
					ReaderState resp = null;
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
								(HashMap<String, Object>) reader.getJobPage(pageParams,nodeConfig.getTransParams(),this.readHandler),
								",complete:" + processPos + "/" + pageList.size(),false,false);

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
			String desc;
			boolean isUpdate = nodeConfig.getPipeParam().getWriteType().equals("increment")?true:false;
			String destName = Common.getInstanceName(instanceName,DataSeq,nodeConfig.getPipeParam().getInstanceName()); 
			if (isFullIndex) {
				createStorePosition(destName, storeId); 
				desc = JOB_TYPE.FULL.name();
			}else {
				desc = JOB_TYPE.INCREMENT.name();
			}
			SQLParam sqlParam = nodeConfig.getPipeParam().getSqlParam();
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
			if(!GlobalParam.FLOW_INFOS.containsKey(instanceName,desc)){
				GlobalParam.FLOW_INFOS.set(instanceName,desc,new HashMap<String, String>());
			} 
			GlobalParam.FLOW_INFOS.get(instanceName,desc).put(instanceName+" seqs nums",String.valueOf(table_seqs.size()));
			for (int i = 0; i < table_seqs.size(); i++) {
				int total = 0;
				ReaderState rState = null;
				long start = Common.getNow();
				String READER_LAST_STAMP = "0";
				String newLastUpdateTime = "0";
				String maxId = "0";
				String startId = "0";
				String tseq = table_seqs.get(i);

				if (null != lastUpdateTimes[i])
					READER_LAST_STAMP = lastUpdateTimes[i];
				try {
					if(READER_LAST_STAMP.equals("null"))
						READER_LAST_STAMP = "0";
					HashMap<String, String> param = new HashMap<String, String>();
					param.put("table", sqlParam.getMainTable());
					param.put("alias", sqlParam.getMainAlias());
					param.put("column", sqlParam.getKeyColumn()); 
					param.put(GlobalParam._start_time, READER_LAST_STAMP);
					param.put(GlobalParam._end_time,"");
					param.put(GlobalParam._seq, tseq); 
					param.put("originalSql", originalSql);
					param.put("pageSql", sqlParam.getPageSql());
					param.put("keyColumnType", sqlParam.getKeyColumnType());
					
				
					if(this.scanHandler!=null)
						this.scanHandler.Handle("",param); 
					//control  repeat with time job 
				do {
					List<String> pageList = reader.getPageSplit(param);
					HashMap<String, String> sqlParams;
					GlobalParam.FLOW_INFOS.get(instanceName,desc).put(instanceName + tseq, "start count page...");
					if (pageList.size() > 0) {
						indexLog("start " + desc, destName, storeId, tseq, "", maxId, READER_LAST_STAMP, 0, "start",
								",totalpage:" + pageList.size());
						int processPos = 0;
						for (String page : pageList) {
							processPos++;
							GlobalParam.FLOW_INFOS.get(instanceName,desc).put(instanceName + tseq,
									processPos + "/" + pageList.size());
							maxId = page;
							sqlParams = null;
							sqlParams = new HashMap<String, String>();
							if (tseq != null && tseq.length() > 0)
								sqlParams.put(GlobalParam._seq, tseq);
							sqlParams.put(GlobalParam._start, startId);
							sqlParams.put(GlobalParam._end, maxId);
							sqlParams.put(GlobalParam._start_time, param.get(GlobalParam._start_time));
							sqlParams.put(GlobalParam._end_time,param.get(GlobalParam._end_time));
							sqlParams.put(GlobalParam._incrementField, incrementField);
							String sql = buildSql(originalSql, sqlParams);

							if ((GlobalParam.FLOW_STATUS.get(instanceName,DataSeq).get() & 4) > 0) {
								indexLog("kill " + desc, instanceName, storeId, tseq, String.valueOf(total), maxId,
										newLastUpdateTime, Common.getNow() - start, "complete", "");
								break;
							} else {
								rState = writeDataSet(desc, destName, storeId, tseq,
										getSqlPageData(sql, incrementField, keyColumn),
										",complete:" + processPos + "/" + pageList.size(), isUpdate,false);

								total += rState.getCount();
								startId = maxId;
							}

							if (newLastUpdateTimes[i] == null || newLastUpdateTimes[i].equals("null")
									|| rState.getReaderScanStamp().compareTo(newLastUpdateTimes[i]) > 0) {
								newLastUpdateTimes[i] = String.valueOf(rState.getReaderScanStamp());
							}
							if (!isFullIndex) {
								GlobalParam.LAST_UPDATE_TIME.set(instanceName,DataSeq, getTimeString(newLastUpdateTimes));
								Common.saveTaskInfo(instanceName, DataSeq, storeId);
							}
						}
						indexLog("complete " + desc, destName, storeId, tseq, String.valueOf(total), maxId,
								newLastUpdateTime, Common.getNow() - start, "complete", "");
						if (nodeConfig.getPipeParam().getNextJob() != null
								&& nodeConfig.getPipeParam().getNextJob().length > 0) {
							ReportStatus.report(instanceName, desc);
						}
					} else {
						indexLog("start " + desc, destName, storeId, tseq, "", maxId, READER_LAST_STAMP, 0, "start",
								" no data job finished!");
					}
				} while (param.get(GlobalParam._end_time).length()>0 && this.scanHandler.needLoop(param));
					 
				} catch (Exception e) { 
					if (isFullIndex) {
						this.writer.getResource();
						try{
							this.writer.remove(destName, storeId);
						}finally{
							this.writer.freeResource(false);
						} 
					} 
					if(e.getMessage().equals("storeId not found")){
						throw new FNException("storeId not found");
					}else{
						log.error("[" + desc +" "+ destName + tseq + "_" + storeId
								+ " ERROR]", e);
						GlobalParam.mailSender.sendHtmlMailBySynchronizationMode(
								" [SearchPlatForm] " + GlobalParam.run_environment,
								"Job " + destName +" "+ desc + " Has stopped!");
						newLastUpdateTimes = lastUpdateTimes;
					}
				}  
			} 
			GlobalParam.FLOW_INFOS.get(instanceName,desc).clear();
			if (isFullIndex){
				int waittime=0;
				while ((GlobalParam.FLOW_STATUS.get(instanceName,DataSeq).get() & 2) > 0) {
					try {
						waittime++;
						Thread.sleep(2000);
						if (waittime > 300) {
							GlobalParam.FLOW_STATUS.get(instanceName,DataSeq).set(4);
							Thread.sleep(10000);
						}
					} catch (InterruptedException e) {
						log.error("currentThreadState InterruptedException", e);
					}
				} 
				switchSearcher(destName, storeId);  
			} 
			
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
		params.put(GlobalParam.READER_SCAN_KEY, incrementField); 
		params.put(GlobalParam.READER_KEY, keyColumn); 
		return (HashMap<String, Object>) reader.getJobPage(params,nodeConfig.getTransParams(),this.readHandler);
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
		if(lastUpdateTime.length()>9 && lastUpdateTime.matches("[0-9]+")){ 
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
		if(this.scanHandler!=null) {
			this.scanHandler.Handle(sql,params);
		}
		Iterator<String> entries = params.keySet().iterator();
		while (entries.hasNext()) {
			String k = entries.next();
			if (k.indexOf("#{") > -1)
				res = res.replace(k, params.get(k));
		}
		return res;
	}
}
