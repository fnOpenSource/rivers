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

	public String run(String instanceName, String storeId, String lastTime, String DataSeq, boolean isFull,
			boolean masterControl) throws FNException {
		if (getInstanceConfig().getReadParams().isSqlType()) {
			return sqlFlow(instanceName, storeId, lastTime, DataSeq, isFull, masterControl);
		} else {
			return noSqlFlow(instanceName, storeId, lastTime, DataSeq, isFull, masterControl);
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
	private String noSqlFlow(String instanceName, String storeId, String lastTime, String DataSeq,
			boolean isFullIndex, boolean masterControl) throws FNException {
		String desc = "increment";
		String destName = Common.getInstanceName(instanceName, DataSeq);
		try {
			NoSQLParam noSqlParam = (NoSQLParam) getInstanceConfig().getReadParams();

			if (lastTime == null || lastTime.equals(""))
				lastTime = "0";
			HashMap<String, String> param = new HashMap<String, String>();
			param.put("table", noSqlParam.getMainTable());
			param.put("column", noSqlParam.getKeyColumn());
			param.put("startTime", lastTime);
			param.put(GlobalParam._incrementField, noSqlParam.getIncrementField());
			List<String> pageList = getReader().getPageSplit(param,getInstanceConfig().getPipeParams().getReadPageSize());
			if (pageList.size() > 0) {
				log.info(Common.formatLog("start " + desc, destName, storeId, "", 0, "", lastTime, 0, "start",
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
					pageParams.put(GlobalParam._start_time, lastTime);
					pageParams.put(GlobalParam._incrementField, noSqlParam.getIncrementField());
					rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, desc, destName, storeId, "",
							getReader().getPageData(pageParams, getInstanceConfig().getWriteFields(), this.readHandler,getInstanceConfig().getPipeParams().getReadPageSize()),
							",process:" + processPos + "/" + pageList.size(), false, false); 

					total += rState.getCount();
					startId = endId;
				}
				log.info(Common.formatLog("complete " + desc, destName, storeId, "", total, "",
						lastTime, Common.getNow() - start, "complete", ""));
			}
		} catch (Exception e) {

		}
		return lastTime;
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
	private String sqlFlow(String instanceName, String storeId, String lastTime, String DataSeq, boolean isFull,
			boolean masterControl) throws FNException {
		String desc;
		boolean isUpdate = getInstanceConfig().getPipeParams().getWriteType().equals("increment") ? true : false;
		String destName = Common.getInstanceName(instanceName, DataSeq);
		String writeTo = masterControl ? getInstanceConfig().getPipeParams().getInstanceName() : destName;
		if (isFull) {
			desc = JOB_TYPE.FULL.name();
		} else {
			desc = JOB_TYPE.INCREMENT.name();
		}
		SQLParam sqlParam = (SQLParam) getInstanceConfig().getReadParams();
		List<String> table_seqs = sqlParam.getSeq().size() > 0 ? sqlParam.getSeq() : Arrays.asList("");
		String originalSql = sqlParam.getSql();
		String incrementField = sqlParam.getIncrementField();
		String keyColumn = sqlParam.getKeyColumn();

		String[] newLastUpdateTimes = new String[table_seqs.size()];
		String[] lastUpdateTimes = new String[table_seqs.size()];
		if (!isFull && lastTime != null && lastTime.split(",").length == table_seqs.size()) {
			newLastUpdateTimes = lastTime.split(",");
			lastUpdateTimes = lastTime.split(",");
		} else {
			Arrays.fill(lastUpdateTimes, lastTime);
		}
		if (!GlobalParam.FLOW_INFOS.containsKey(instanceName, desc)) {
			GlobalParam.FLOW_INFOS.set(instanceName, desc, new HashMap<String, String>());
		}
		GlobalParam.FLOW_INFOS.get(instanceName, desc).put(instanceName + " seqs nums",
				String.valueOf(table_seqs.size()));
		for (int i = 0; i < table_seqs.size(); i++) {
			int total = 0;
			ReaderState rState = null;
			long start = Common.getNow();
			String READER_LAST_STAMP = "0";
			String dataBoundary = "0";
			String startId = "0";
			String tseq = table_seqs.get(i);

			if (null != lastUpdateTimes[i])
				READER_LAST_STAMP = lastUpdateTimes[i];
			try {
				if (READER_LAST_STAMP.equals("null"))
					READER_LAST_STAMP = "0";
				HashMap<String, String> param = new HashMap<String, String>();
				param.put("table", sqlParam.getMainTable());
				param.put("alias", sqlParam.getMainAlias());
				param.put("column", sqlParam.getKeyColumn());
				param.put(GlobalParam._start_time, READER_LAST_STAMP);
				param.put(GlobalParam._end_time, "");
				param.put(GlobalParam._seq, tseq);
				param.put("originalSql", originalSql);
				param.put("pageSql", sqlParam.getPageScan());
				param.put("keyColumnType", sqlParam.getKeyColumnType());

				if (this.readHandler != null)
					this.readHandler.handlePage("", param);
				// control repeat with time job
				do { 
					GlobalParam.FLOW_INFOS.get(instanceName, desc).put(instanceName + tseq, "start count page..."); 
					getReader().lock.lock();
					List<String> pageList = getReader().getPageSplit(param,getInstanceConfig().getPipeParams().getReadPageSize());
					getReader().lock.unlock();
					if (pageList == null)
						throw new FNException("read data get page split exception!");
					if (pageList.size() > 0) {
						log.info(Common.formatLog("Start " + desc, destName, storeId, tseq, 0, dataBoundary,
								READER_LAST_STAMP, 0, "start", ",totalpage:" + pageList.size()));
						int processPos = 0;
						for (String page : pageList) {
							processPos++;
							GlobalParam.FLOW_INFOS.get(instanceName, desc).put(instanceName + tseq,
									processPos + "/" + pageList.size());
							dataBoundary = page;
							String sql = SqlUtil.fillParam(originalSql,
									SqlUtil.getScanParam(tseq, startId, dataBoundary,
											param.get(GlobalParam._start_time), param.get(GlobalParam._end_time),
											incrementField)); 
							if (Common.checkFlowStatus(instanceName, DataSeq, desc, STATUS.Termination)) {
								throw new FNException(instanceName + " " + desc + " job has been Terminated!");
							} else {
								DataPage pagedata; 
								if (getInstanceConfig().openCompute()) { 
									getReader().lock.lock();
									pagedata = (DataPage) CPU.RUN(getID(), "Pipe", "fetchDataSet", false, sql, incrementField, keyColumn,
											getInstanceConfig().getComputeFields(),getReader(),this.readHandler);  
									getReader().freeJobPage();
									getReader().lock.unlock();
									pagedata = (DataPage) CPU.RUN(getID(), "ML", "computeDataSet", false, getID(), desc, writeTo, pagedata); 
									if(processPos==pageList.size()) {
										rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, desc, writeTo, storeId, tseq, pagedata,
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
									rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, desc, writeTo, storeId, tseq, pagedata,
											",process:" + processPos + "/" + pageList.size(), isUpdate, false); 
								 
								} 
								if (rState.isStatus() == false)
									throw new FNException("read data exception!");
								total += rState.getCount();
								startId = dataBoundary;
							}

							if (newLastUpdateTimes[i] == null
									|| rState.getReaderScanStamp().compareTo(newLastUpdateTimes[i]) > 0) {
								newLastUpdateTimes[i] = rState.getReaderScanStamp();
							}
							if (!isFull) {
								GlobalParam.LAST_UPDATE_TIME.set(instanceName, DataSeq,
										Common.arrayToString(newLastUpdateTimes,","));
								Common.saveTaskInfo(instanceName, DataSeq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
							}
						}
						log.info(Common.formatLog("Complete " + desc, destName, storeId, tseq, total,
								dataBoundary, READER_LAST_STAMP, Common.getNow() - start, "complete", "")); 
					} else {
						log.info(Common.formatLog("Complete " + desc, destName, storeId, tseq,0, dataBoundary,
								READER_LAST_STAMP, 0, "start", " no data!"));
					}
				} while (param.get(GlobalParam._end_time).length() > 0 && this.readHandler.loopScan(param));

			} catch (Exception e) {
				if (isFull && !masterControl) {
					for (int t = 0; t < 5; t++) {
						getWriter().PREPARE(false, false);
						if (getWriter().ISLINK()) {
							try {
								getWriter().removeInstance(destName, storeId);
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
					log.error("[" + desc + " " + destName + tseq + "_" + storeId + " ERROR]", e);
					GlobalParam.mailSender.sendHtmlMailBySynchronizationMode(" [Rivers] " + GlobalParam.run_environment,
							"Job " + destName + " " + desc + " Has stopped!");
					newLastUpdateTimes = lastUpdateTimes;
				}
			}
		}

		GlobalParam.FLOW_INFOS.get(instanceName, desc).clear(); 
		if (isFull) {
			if (masterControl) {
				String _dest = getInstanceConfig().getPipeParams().getInstanceName();
				synchronized (GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())) {
					String remainJobs = GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_JOBS.name());
					remainJobs = remainJobs.replace(destName, "").trim();
					GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
							.put(GlobalParam.FLOWINFO.FULL_JOBS.name(), remainJobs);
					if (remainJobs.length() == 0) {
						String _storeId = GlobalParam.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
								.get(GlobalParam.FLOWINFO.FULL_STOREID.name());
						PipePump ts = GlobalParam.SOCKET_CENTER.getPipePump(_dest, null, false,
								GlobalParam.FLOW_TAG._DEFAULT.name());
						CPU.RUN(ts.getID(), "Pond", "switchInstance", true, instanceName, DataSeq, _storeId);
					}
				}
			} else {
				CPU.RUN(getID(), "Pond", "switchInstance", true, instanceName, DataSeq, storeId);
			}
		}

		return Common.arrayToString(newLastUpdateTimes,",");
	}
 
}
