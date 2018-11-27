package com.feiniu.piper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.JOB_TYPE;
import com.feiniu.config.GlobalParam.STATUS;
import com.feiniu.config.InstanceConfig;
import com.feiniu.instruction.Instruction;
import com.feiniu.model.reader.DataPage;
import com.feiniu.model.reader.ReaderState;
import com.feiniu.node.CPU;
import com.feiniu.param.warehouse.NoSQLParam;
import com.feiniu.param.warehouse.SQLParam;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.reader.handler.Handler;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.util.SqlUtil;
import com.feiniu.writer.WriterFlowSocket;
 
/**
 * PipePump is the energy of the flow pipes 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-23 14:36
 */
public final class PipePump extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("PipePump");
	/** defined custom read flow socket */
	private Handler readHandler;

	public static PipePump getInstance(ReaderFlowSocket reader, WriterFlowSocket writer,
			InstanceConfig instanceConfig) {
		return new PipePump(reader, writer, instanceConfig);
	}

	private PipePump(ReaderFlowSocket reader, WriterFlowSocket writer, InstanceConfig instanceConfig) {
		CPU.prepare(getID(), instanceConfig, writer, reader);
		try {
			if (instanceConfig.getPipeParams().getReadHandler() != null) {
				this.readHandler = (Handler) Class.forName(instanceConfig.getPipeParams().getReadHandler())
						.newInstance();
			}
		} catch (Exception e) {
			log.error("PipePump init Exception,", e);
		}
	}

	public void run(String instanceName, String storeId, String DataSeq, boolean isFull,
			boolean masterControl) throws FNException {
		if (getInstanceConfig().getReadParams().isSqlType()) {
			sqlFlow(instanceName, storeId, DataSeq, isFull, masterControl);
		} else {
			noSqlFlow(instanceName, storeId, DataSeq, isFull, masterControl);
		}
	}

	public InstanceConfig getInstanceConfig() {
		return CPU.getContext(getID()).getInstanceConfig();
	}

	public ReaderFlowSocket getReader() {
		return CPU.getContext(getID()).getReader();
	}

	public WriterFlowSocket getWriter() {
		return CPU.getContext(getID()).getWriter();
	} 
 
	/**
	 * write to not db platform
	 * 
	 * @param indexName
	 * @param storeId
	 * @param lastTime
	 * @param isFullIndex
	 * @return
	 */
	private void noSqlFlow(String instanceName, String storeId, String DataSeq,
			boolean isFullIndex, boolean masterControl) throws FNException {
		String desc = "increment";
		String destName = Common.getMainName(instanceName, DataSeq);
		try {
			NoSQLParam noSqlParam = (NoSQLParam) getInstanceConfig().getReadParams();
 
			HashMap<String, String> param = new HashMap<String, String>();
			param.put("table", noSqlParam.getMainTable());
			param.put("column", noSqlParam.getKeyColumn());
			param.put("startTime", "0");
			param.put(GlobalParam._incrementField, noSqlParam.getIncrementField());
			List<String> pageList = getReader().getPageSplit(param,getInstanceConfig().getPipeParams().getReadPageSize());
			if (pageList.size() > 0) {
				log.info(Common.formatLog("start","start " + desc, destName, storeId, "", 0, "", "0", 0, 
						",totalpage:" + pageList.size()));
				int processPos = 0;
				String startId = "";
				String endId = "";
				int total = 0;
				ReaderState rState;
				long start = Common.getNow();
				for (String page : pageList) {
					processPos++;
					endId = page;
					HashMap<String, String> pageParams = new HashMap<String, String>();
					pageParams.put(GlobalParam._start, startId);
					pageParams.put(GlobalParam._end, endId);
					pageParams.put(GlobalParam._start_time, "0");
					pageParams.put(GlobalParam._incrementField, noSqlParam.getIncrementField());
					rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, desc, destName, storeId, "",
							getReader().getPageData(pageParams, getInstanceConfig().getWriteFields(), this.readHandler,getInstanceConfig().getPipeParams().getReadPageSize()),
							",process:" + processPos + "/" + pageList.size(), false, false); 

					total += rState.getCount();
					startId = endId;
				}
				log.info(Common.formatLog("complete","complete " + desc, destName, storeId, "", total, "",
						"0", Common.getNow() - start,""));
			}
		} catch (Exception e) {

		} 
	}

	/**
	 * do Sql resource data Write
	 * 
	 * @param storeId
	 * @param lastTime
	 *            get sql data filter with scan last timestamp
	 * @param lastBatchId
	 * @param DataSeq
	 *            for series data source sequence
	 * @param instanceName
	 *            data source main tag name
	 * @return String last update value
	 */
	private void sqlFlow(String instance, String storeId, String DataSeq, boolean isFull,
			boolean masterControl) throws FNException {
		JOB_TYPE job_type; 
		String mainName = Common.getMainName(instance, DataSeq);
		String writeTo = masterControl ? getInstanceConfig().getPipeParams().getInstanceName() : mainName;
		if (isFull) {
			job_type = JOB_TYPE.FULL;
		} else {
			job_type = JOB_TYPE.INCREMENT;
		}
		SQLParam sqlParam = (SQLParam) getInstanceConfig().getReadParams();
		List<String> table_seqs = sqlParam.getSeq().size() > 0 ? sqlParam.getSeq() : Arrays.asList("");
		String originalSql = sqlParam.getSql();
		 
		if (!GlobalParam.FLOW_INFOS.containsKey(instance, job_type.name())) {
			GlobalParam.FLOW_INFOS.set(instance, job_type.name(), new HashMap<String, String>());
		}
		GlobalParam.FLOW_INFOS.get(instance, job_type.name()).put(instance + " seqs nums",
				String.valueOf(table_seqs.size()));
		for (String tseq:table_seqs) {  
			try { 
				HashMap<String, String> param = new HashMap<String, String>();
				param.put("table", sqlParam.getMainTable());
				param.put("alias", sqlParam.getMainAlias());
				param.put("column", sqlParam.getKeyColumn());
				param.put(GlobalParam._start_time, isFull?Common.getFullStartInfo(instance, DataSeq):GlobalParam.SCAN_POSITION.get(mainName).getSeqPos(tseq));
				param.put(GlobalParam._end_time, "");
				param.put(GlobalParam._seq, tseq);
				param.put("originalSql", originalSql);
				param.put("pageSql", sqlParam.getPageScan());
				param.put("keyColumnType", sqlParam.getKeyColumnType());

				if (this.readHandler != null)
					this.readHandler.handlePage("", param);
				// control repeat with time job
				do { 
					GlobalParam.FLOW_INFOS.get(instance, job_type.name()).put(instance + tseq, "start count page..."); 
					getReader().lock.lock();
					List<String> pageList = getReader().getPageSplit(param,getInstanceConfig().getPipeParams().getReadPageSize());
					getReader().lock.unlock();
					if (pageList == null)
						throw new FNException("read data get page split exception!");
					if (pageList.size() > 0) {
						log.info(Common.formatLog("start","Start " + job_type.name(), mainName, storeId, tseq,0, "",
								GlobalParam.SCAN_POSITION.get(mainName).getSeqPos(tseq), 0,",totalpage:" + pageList.size()));
						if(getInstanceConfig().getPipeParams().isMultiThread()) {
							
						}else {
							singleThread(instance,mainName,tseq,job_type,storeId,DataSeq,tseq,originalSql,pageList,param,sqlParam,writeTo);
						} 
					} else {
						log.info(Common.formatLog("start","Complete " + job_type.name(), mainName, storeId, tseq,0, "",
								GlobalParam.SCAN_POSITION.get(mainName).getSeqPos(tseq), 0, " no data!"));
					}
				} while (param.get(GlobalParam._end_time).length() > 0 && this.readHandler.loopScan(param));

			} catch (Exception e) {
				if (isFull && !masterControl) {
					for (int t = 0; t < 5; t++) {
						getWriter().PREPARE(false, false);
						if (getWriter().ISLINK()) {
							try {
								getWriter().removeInstance(mainName, storeId);
							} finally {
								getWriter().REALEASE(false, false);
							}
							break;
						}
					}
				}
				if (e.getMessage() != null && e.getMessage().equals("storeId not found")) {
					throw new FNException("storeId not found");
				} else {
					log.error("[" + job_type.name() + " " + mainName + tseq + "_" + storeId + " ERROR]", e);
					GlobalParam.mailSender.sendHtmlMailBySynchronizationMode(" [Rivers] " + GlobalParam.run_environment,
							"Job " + mainName + " " + job_type.name() + " Has stopped!"); 
				}
			}
		}

		GlobalParam.FLOW_INFOS.get(instance, job_type.name()).clear(); 
		if (isFull) {
			if (masterControl) {
				String _dest = getInstanceConfig().getPipeParams().getInstanceName();
				synchronized (GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())) {
					String remainJobs = GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_JOBS.name());
					remainJobs = remainJobs.replace(mainName, "").trim();
					GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
							.put(GlobalParam.FLOWINFO.FULL_JOBS.name(), remainJobs);
					if (remainJobs.length() == 0) {
						String _storeId = GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
								.get(GlobalParam.FLOWINFO.FULL_STOREID.name());
						PipePump ts = GlobalParam.SOCKET_CENTER.getPipePump(_dest, null, false,
								GlobalParam.FLOW_TAG._DEFAULT.name());
						CPU.RUN(ts.getID(), "Pond", "switchInstance", true, instance, DataSeq, _storeId);
					}
				}
			} else {
				CPU.RUN(getID(), "Pond", "switchInstance", true, instance, DataSeq, storeId);
			}
		} 
	}
	
	/**
	 * support recover job type
	 * @param pageList
	 * @throws FNException 
	 */
	private void singleThread(String instanceName,String mainName,String seq,JOB_TYPE job_type,String storeId,String DataSeq,String tseq,String originalSql,List<String> pageList,HashMap<String, String> param,SQLParam sqlParam,String writeTo) throws FNException { 
		ReaderState rState = null;
		int processPos = 0;
		String startId = "0";
		String dataBoundary = "";
		int total = 0; 
		long start = Common.getNow();
		boolean isUpdate = getInstanceConfig().getPipeParams().getWriteType().equals("increment") ? true : false;
		String incrementField = sqlParam.getIncrementField();
		String keyColumn = sqlParam.getKeyColumn();
		for (String page : pageList) {
			processPos++;
			GlobalParam.FLOW_INFOS.get(instanceName, job_type.name()).put(instanceName + tseq,
					processPos + "/" + pageList.size());
			dataBoundary = page;
			String sql = SqlUtil.fillParam(originalSql,
					SqlUtil.getScanParam(tseq, startId, dataBoundary,
							param.get(GlobalParam._start_time), param.get(GlobalParam._end_time),
							incrementField)); 
			if (Common.checkFlowStatus(instanceName, DataSeq, job_type, STATUS.Termination)) {
				throw new FNException(instanceName + " " + job_type.name() + " job has been Terminated!");
			} else {
				DataPage pagedata; 
				if (getInstanceConfig().openCompute()) { 
					getReader().lock.lock();
					pagedata = (DataPage) CPU.RUN(getID(), "Pipe", "fetchDataSet", false, sql, incrementField, keyColumn,
							getInstanceConfig().getComputeFields(),getReader(),this.readHandler);  
					getReader().freeJobPage();
					getReader().lock.unlock();
					pagedata = (DataPage) CPU.RUN(getID(), "ML", "computeDataSet", false, getID(), job_type.name(), writeTo, pagedata); 
					if(processPos==pageList.size()) {
						rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, job_type.name(), writeTo, storeId, tseq, pagedata,
								",process:" + processPos + "/" + pageList.size(), isUpdate, false);  
					}else {
						continue; 
					} 
				} else { 
					getReader().lock.lock();
					pagedata = (DataPage) CPU.RUN(getID(), "Pipe", "fetchDataSet", false, sql, incrementField, keyColumn,
							getInstanceConfig().getWriteFields(),getReader(),this.readHandler);  
					getReader().freeJobPage();
					getReader().lock.unlock();
					rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, job_type.name(), writeTo, storeId, tseq, pagedata,
							",process:" + processPos + "/" + pageList.size(), isUpdate, false);  
				} 
				if (rState.isStatus() == false)
					throw new FNException("read data exception!");
				total += rState.getCount();
				startId = dataBoundary;
			}
			
			if (rState.getReaderScanStamp().compareTo(GlobalParam.SCAN_POSITION.get(instanceName).getSeqPos(seq)) > 0) {
				GlobalParam.SCAN_POSITION.get(instanceName).updateSeqPos(seq, rState.getReaderScanStamp());
			}
			if (job_type==JOB_TYPE.INCREMENT) { 
				Common.saveTaskInfo(instanceName, DataSeq,storeId,GlobalParam.JOB_INCREMENTINFO_PATH);
			}
		}
		log.info(Common.formatLog("complete", "Complete " + job_type.name(), mainName, storeId, tseq, total,
				dataBoundary, GlobalParam.SCAN_POSITION.get(mainName).getSeqPos(tseq), Common.getNow() - start, "")); 
	}
}
