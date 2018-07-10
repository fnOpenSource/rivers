package com.feiniu.instruction.flow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.JOB_TYPE;
import com.feiniu.config.InstanceConfig;
import com.feiniu.correspond.ReportStatus;
import com.feiniu.instruction.Instruction;
import com.feiniu.model.ReaderState;
import com.feiniu.model.param.MessageParam;
import com.feiniu.model.param.NOSQLParam;
import com.feiniu.model.param.SQLParam;
import com.feiniu.node.CPU;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.reader.handler.Handler;
import com.feiniu.reader.util.DataSetReader;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.writer.WriterFlowSocket;

/**
 * Build river flow transfer data from A to B
 * @author chengwen
 * @version 2.0 
 */
public final class TransDataFlow extends Instruction{

	private final static Logger log = LoggerFactory.getLogger("TransDataFlow");   
	/**defined custom read flow socket*/
	private Handler readHandler; 
	private Handler scanHandler;   
	
	public static TransDataFlow getInstance(ReaderFlowSocket<?> reader,WriterFlowSocket writer, InstanceConfig instanceConfig){
		return new TransDataFlow(reader,writer,instanceConfig);
	}
 
	private TransDataFlow(ReaderFlowSocket<?> reader,WriterFlowSocket writer, InstanceConfig instanceConfig) { 
		CPU.prepare(getID(), instanceConfig, writer,reader);  
		try {
			if(instanceConfig.getPipeParam().getDataFromhandler()!=null){
				this.readHandler = (Handler) Class.forName(instanceConfig.getPipeParam().getDataFromhandler()).newInstance();
			} 
			if(instanceConfig.getPipeParam().getSqlParam().getHandler()!=null) {
				this.scanHandler = (Handler) Class.forName(instanceConfig.getPipeParam().getSqlParam().getHandler()).newInstance();
			}
		}catch(Exception e){
			log.error("TransDataFlow Instruction Exception,",e);
		}
	}   
	
	public String run(String instanceName, String storeId, String lastTime,
			String DataSeq, boolean isFullIndex) throws FNException{
		if(getInstanceConfig().getPipeParam().getNoSqlParam()!=null){
			return doNosqlWrite(instanceName, storeId, lastTime, isFullIndex);
		}else{
			return doSqlWrite(instanceName, storeId, lastTime, DataSeq, isFullIndex);
		}
	}  
	
	public InstanceConfig getInstanceConfig() {
		return CPU.getContext(getID()).getInstanceConfig();
	}
	
	public ReaderFlowSocket<?> getReader() {
		return CPU.getContext(getID()).getReader();
	}
	
	public WriterFlowSocket getWriter() {
		return CPU.getContext(getID()).getWriter();
	}

	/**
	 * message scan from db write into index 
	 */
	public void scanDbWrite(String instanceName, String storeId,
			String dbseq, HashMap<String, String> params) throws Exception {
		MessageParam MP = getInstanceConfig().getMessageParam();
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
 * @return ReaderState 
 * @throws Exception
 */  
	public ReaderState writeDataSet(String id,String instance, String storeId,
			String seq, HashMap<String, Object> dataSet, String info,boolean isUpdate,boolean monopoly) throws Exception {
		ReaderState rstate = new ReaderState(); 
		DataSetReader DSReader = new DataSetReader();
		DSReader.init(dataSet);
		long start = Common.getNow();
		int num = 0; 
		if (DSReader.status()) {
			boolean _connect;
			if(monopoly) {
				_connect = getWriter().MONOPOLY();
			}else {
				_connect = getWriter().LINK();
			} 
			if(!_connect) {
				rstate.setStatus(false);
				return rstate;
			}
			boolean freeConn = false;
			try{
				while (DSReader.nextLine()) {   
					getWriter().write(DSReader.getkeyColumn(),DSReader.getLineData(),getInstanceConfig().getTransParams(),instance, storeId,isUpdate);
					num++;
				}
				String READER_LAST_STAMP = DSReader.getScanStamp();
				String maxId = DSReader.getMaxId();
				DSReader.close();
				rstate.setReaderScanStamp(READER_LAST_STAMP);
				rstate.setMaxId(maxId);
				rstate.setCount(num);
				log.info(Common.formatLog(" -- "+id+" onepage ",instance, storeId, seq, String.valueOf(num), maxId,
						String.valueOf(READER_LAST_STAMP), Common.getNow() - start, "onepage", info)); 
			}catch(Exception e){
				if(e.getMessage().equals("storeId not found")){
					throw new FNException("storeId not found");
				}else{
					freeConn = true;
				}
			}finally{
				getWriter().flush();
				if(!monopoly) 
					getWriter().REALEASE(freeConn);
			}  
		}else {
			rstate.setStatus(false);
		}
		getReader().freeJobPage();
		return rstate;
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
			String indexName = Common.getInstanceName(instanceName,"",getInstanceConfig().getPipeParam().getInstanceName()); 
			
			try {
				if (isFullIndex) {
					CPU.RUN(getID(), "Pond", "createStorePosition",indexName, storeId); 
					desc = "full";
				} 
				NOSQLParam noSqlParam = getInstanceConfig().getPipeParam().getNoSqlParam();
				
				if(lastTime==null || lastTime.equals(""))
					lastTime = "0";
				HashMap<String, String> param = new HashMap<String, String>();
				param.put("table", noSqlParam.getMainTable()); 
				param.put("column", noSqlParam.getKeyColumn());
				param.put("startTime", lastTime);  
				param.put(GlobalParam._incrementField, noSqlParam.getIncrementField());  
				List<String> pageList = getReader().getPageSplit(param); 
				if (pageList.size() > 0) {
					log.info(Common.formatLog("start " + desc, indexName, storeId, "", "",
							"", lastTime, 0, "start", ",totalpage:"
									+ pageList.size()));
					int processPos = 0;
					String startId ="";
					String endId = "";
					int total = 0;
					ReaderState rstate;
					long start = Common.getNow();
					for (String page : pageList) { 
						processPos++;
						endId = page;
						HashMap<String, String> pageParams = new HashMap<String, String>(); 
						pageParams.put(GlobalParam._start, startId);
						pageParams.put(GlobalParam._end, endId);
						pageParams.put(GlobalParam._start_time, lastTime);
						pageParams.put(GlobalParam._incrementField, noSqlParam.getIncrementField());
						 
						rstate = writeDataSet(desc,indexName, storeId, "",
								(HashMap<String, Object>) getReader().getJobPage(pageParams,getInstanceConfig().getTransParams(),this.readHandler),
								",complete:" + processPos + "/" + pageList.size(),false,false);
						
						total += rstate.getCount();
						startId = endId;
					}
					log.info(Common.formatLog("complete " + desc, indexName, storeId, "",
							String.valueOf(total), "", lastTime,
							Common.getNow() - start, "complete", ""));
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
			boolean isUpdate = getInstanceConfig().getPipeParam().getWriteType().equals("increment")?true:false;
			String destName = Common.getInstanceName(instanceName,DataSeq,getInstanceConfig().getPipeParam().getInstanceName()); 
			if (isFullIndex) {
				CPU.RUN(getID(), "Pond", "createStorePosition",destName, storeId); 
				desc = JOB_TYPE.FULL.name();
			}else {
				desc = JOB_TYPE.INCREMENT.name();
			}
			SQLParam sqlParam = getInstanceConfig().getPipeParam().getSqlParam();
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
					List<String> pageList = getReader().getPageSplit(param);
					HashMap<String, String> sqlParams;
					GlobalParam.FLOW_INFOS.get(instanceName,desc).put(instanceName + tseq, "start count page...");
					if (pageList.size() > 0) {
						log.info(Common.formatLog("start " + desc, destName, storeId, tseq, "", maxId, READER_LAST_STAMP, 0, "start",
								",totalpage:" + pageList.size()));
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
								log.info(Common.formatLog("kill " + desc, instanceName, storeId, tseq, String.valueOf(total), maxId,
										newLastUpdateTime, Common.getNow() - start, "complete", ""));
								break;
							} else {
								rState = writeDataSet(desc, destName, storeId, tseq,
										getSqlPageData(sql, incrementField, keyColumn),
										",complete:" + processPos + "/" + pageList.size(), isUpdate,false); 
								if(rState.isStatus()==false)
									break;
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
						log.info(Common.formatLog("complete " + desc, destName, storeId, tseq, String.valueOf(total), maxId,
								newLastUpdateTime, Common.getNow() - start, "complete", ""));
						if (getInstanceConfig().getPipeParam().getNextJob() != null
								&& getInstanceConfig().getPipeParam().getNextJob().length > 0) {
							ReportStatus.report(instanceName, desc);
						}
					} else {
						log.info(Common.formatLog("start " + desc, destName, storeId, tseq, "", maxId, READER_LAST_STAMP, 0, "start",
								" no data job finished!"));
					}
				} while (param.get(GlobalParam._end_time).length()>0 && this.scanHandler.needLoop(param));
					 
				} catch (Exception e) { 
					if (isFullIndex) {
						getWriter().LINK();
						try{
							getWriter().removeInstance(destName, storeId);
						}finally{
							getWriter().REALEASE(false);
						} 
					} 
					if(e.getMessage()!=null && e.getMessage().equals("storeId not found")){ 
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
				CPU.RUN(getID(), "Pond", "switchInstance", destName, storeId);  
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
		return (HashMap<String, Object>) getReader().getJobPage(params,getInstanceConfig().getTransParams(),this.readHandler);
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
