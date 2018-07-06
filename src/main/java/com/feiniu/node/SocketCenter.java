package com.feiniu.node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.instruction.flow.TransDataFlow;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.model.param.WarehouseSqlParam;
import com.feiniu.reader.flow.ReaderFlowSocket;
import com.feiniu.reader.flow.ReaderFlowSocketFactory;
import com.feiniu.searcher.Searcher;
import com.feiniu.searcher.SearcherFactory;
import com.feiniu.searcher.flow.SearcherFlowSocket;
import com.feiniu.util.Common;
import com.feiniu.writer.WriterFactory;
import com.feiniu.writer.flow.WriterFlowSocket;

/**
 * data-flow router reader searcher and writer control center seq only support
 * for reader to read series data source,and create one or more instance in
 * writer single destination.
 * 
 * @author chengwen
 * @version 1.0
 */
public final class SocketCenter {

	private Map<String, TransDataFlow> transDataFlowMap = new ConcurrentHashMap<String, TransDataFlow>();
	private Map<String, WriterFlowSocket> writerSocketMap = new ConcurrentHashMap<String, WriterFlowSocket>();
	private Map<String, ReaderFlowSocket<?>> readerSocketMap = new ConcurrentHashMap<String, ReaderFlowSocket<?>>();
	private Map<String, SearcherFlowSocket> searcherSocketMap = new ConcurrentHashMap<String, SearcherFlowSocket>();
	private Map<String, Searcher> searcherMap = new ConcurrentHashMap<String, Searcher>();

	private final static Logger log = LoggerFactory.getLogger(SocketCenter.class);

	public Searcher getSearcher(String instanceName) {
		if (!searcherMap.containsKey(instanceName)) {
			if (!GlobalParam.nodeConfig.getSearchConfigs().containsKey(instanceName))
				return null;
			InstanceConfig instanceConfig = GlobalParam.nodeConfig.getSearchConfigs().get(instanceName);
			Searcher searcher = Searcher.getInstance(instanceName, instanceConfig, getSearcherSocket(instanceName));
			searcherMap.put(instanceName, searcher);
		}
		return searcherMap.get(instanceName);
	}

	/**
	 * get Writer auto look for sql and nosql maps to get socket each writer has
	 * sperate flow
	 * 
	 * @param seq
	 *            for series data source sequence
	 * @param instanceName
	 *            data source main tag name
	 * @param needClear
	 *            for reset resource
	 * @param tag
	 *            Marking resource
	 */
	public TransDataFlow getTransDataFlow(String instance, String seq, boolean needClear, String tag) {
		if (!transDataFlowMap.containsKey(Common.getInstanceName(instance, seq, null,tag) + tag) || needClear) { 
			TransDataFlow transDataFlow  = TransDataFlow.getInstance(getReaderSocket(instance, seq,tag), getWriterSocket(instance, seq,tag),
					GlobalParam.nodeConfig.getInstanceConfigs().get(instance));
			transDataFlowMap.put(Common.getInstanceName(instance, seq, null,tag), transDataFlow);
		}
		return transDataFlowMap.get(Common.getInstanceName(instance, seq, null,tag));
	}

	public WarehouseParam getWHP(String destination) {
		WarehouseParam param = null;
		if (GlobalParam.nodeConfig.getNoSqlParamMap().containsKey(destination)) {
			param = GlobalParam.nodeConfig.getNoSqlParamMap().get(destination);
		} else if (GlobalParam.nodeConfig.getSqlParamMap().containsKey(destination)) {
			param = GlobalParam.nodeConfig.getSqlParamMap().get(destination);
		}
		return param;
	}

	public ReaderFlowSocket<?> getReaderSocket(String instance, String seq,String tag) {
		if (!readerSocketMap.containsKey(Common.getInstanceName(instance, seq, null,tag))) {
			ReaderFlowSocket<?> reader;
			String readFrom = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().getDataFrom();
			if (GlobalParam.nodeConfig.getNoSqlParamMap().get(readFrom) != null) {
				Map<String, WarehouseNosqlParam> dataMap = GlobalParam.nodeConfig.getNoSqlParamMap();
				if (!dataMap.containsKey(readFrom)) {
					log.error("data source config " + readFrom + " not exists");
					return null;
				}
				reader = ReaderFlowSocketFactory.getChannel(dataMap.get(readFrom), seq);
			} else {
				Map<String, WarehouseSqlParam> dataMap = GlobalParam.nodeConfig.getSqlParamMap();
				if (!dataMap.containsKey(readFrom)) {
					log.error("data source config " + readFrom + " not exists");
					return null;
				}
				reader = ReaderFlowSocketFactory.getChannel(dataMap.get(readFrom), seq);
			}
			readerSocketMap.put(Common.getInstanceName(instance, seq, null,tag), reader);
		}
		return readerSocketMap.get(Common.getInstanceName(instance, seq, null,tag));
	}

	public WriterFlowSocket getWriterSocket(String instance, String seq,String tag) {
		if (!writerSocketMap.containsKey(instance)) {
			WarehouseParam param = getWHP(
					GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().getWriteTo());
			if (param == null)
				return null;
			writerSocketMap.put(instance, WriterFactory.getWriter(param, seq));
		}
		return writerSocketMap.get(instance);
	}

	private SearcherFlowSocket getSearcherSocket(String secname) {
		if (!searcherSocketMap.containsKey(secname)) {
			WarehouseParam param = getWHP(
					GlobalParam.nodeConfig.getSearchConfigs().get(secname).getPipeParam().getSearcher());
			if (param == null)
				return null;

			InstanceConfig paramConfig = GlobalParam.nodeConfig.getSearchConfigs().get(secname);
			SearcherFlowSocket searcher = SearcherFactory.getSearcherFlow(param, paramConfig, null);
			searcherSocketMap.put(secname, searcher);
		}
		return searcherSocketMap.get(secname); 
	}

}
