package com.feiniu.node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.model.param.WarehouseSqlParam;
import com.feiniu.searcher.FNQueryBuilder;
import com.feiniu.searcher.FNSearcher;
import com.feiniu.searcher.FNSearcherSocketFactory;
import com.feiniu.searcher.flow.SearcherFlowSocket;
import com.feiniu.util.Common;
import com.feiniu.writer.WriterFactory;
import com.feiniu.writer.flow.JobWriter;
import com.feiniu.writer.flow.WriterFlowSocket;
import com.feiniu.writer.jobFlow.WriteFlowSocket;
import com.feiniu.writer.jobFlow.WriteFlowSocketFactory;

/**
 * data-flow router reader and writer control center
 * seq only support for reader to read series data source,and create 
 * one or more instance in writer single destination.
 * @author chengwen
 * @version 1.0 
 */
public class NodeCenter{  
	 
	private Map<String,JobWriter> writerChannelMap = new ConcurrentHashMap<String, JobWriter>();
	private Map<String, WriterFlowSocket> destinationWriterMap = new ConcurrentHashMap<String, WriterFlowSocket>();
	private Map<String, SearcherFlowSocket> searcherMap = new ConcurrentHashMap<String, SearcherFlowSocket>(); 
	
	private final static Logger log = LoggerFactory.getLogger(NodeCenter.class);  
	  
	
	/**
	 * get Writer auto look for sql and nosql maps
	 * @param seq for series data source sequence
	 * @param instanceName data source main tag name
	 */ 
	public JobWriter getWriterChannel(String instanceName, String seq,boolean needClear) { 
		if(!writerChannelMap.containsKey(Common.getInstanceName(instanceName, seq)) || needClear){
			NodeConfig paramConfig = GlobalParam.nodeTreeConfigs.getConfigMap().get(instanceName);
			JobWriter writer=null;
			String readFrom = paramConfig.getTransParam().getDataFrom(); 
			WriteFlowSocket<?> flowSocket;
			if(GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(readFrom)!=null){ 
				Map<String, WarehouseNosqlParam> dataMap = GlobalParam.nodeTreeConfigs.getNoSqlParamMap();
				if (!dataMap.containsKey(readFrom)){
					log.error("data source config " + readFrom + " not exists");
					return null;
				}   
				flowSocket = WriteFlowSocketFactory.getChannel(dataMap.get(readFrom),seq);
			}else{ 
				Map<String, WarehouseSqlParam> dataMap = GlobalParam.nodeTreeConfigs.getSqlParamMap();
				if (!dataMap.containsKey(readFrom)){
					log.error("data source config " + readFrom + " not exists");
					return null;
				}   
				flowSocket = WriteFlowSocketFactory.getChannel(dataMap.get(readFrom),seq);
			} 
			writer = JobWriter.getInstance(flowSocket,getDestinationWriter(instanceName), paramConfig);
			writerChannelMap.put(Common.getInstanceName(instanceName, seq), writer);
		}
		return writerChannelMap.get(Common.getInstanceName(instanceName, seq)); 
	} 
 
	public FNSearcher getSearcher(String instanceName) {
		
		if (!GlobalParam.nodeTreeConfigs.getSearchConfigs().containsKey(instanceName))
			return null;
		
		NodeConfig paramConfig = GlobalParam.nodeTreeConfigs.getSearchConfigs().get(instanceName);
		FNSearcher searcher = FNSearcher.getInstance(instanceName, paramConfig, getIndexSearcher(instanceName));
		return searcher;
	}
 
	public FNQueryBuilder getQueryBuilder(String instanceName) { 
		if (!GlobalParam.nodeTreeConfigs.getConfigMap().containsKey(instanceName))
			return null;
		
		WarehouseNosqlParam param = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(GlobalParam.nodeTreeConfigs.getConfigMap().get(instanceName).getTransParam().getWriteTo());
		if (param == null)
			return null;
		
		if (param.getType() == DATA_TYPE.ES){		
			return null;
		}
		else if (param.getType() == DATA_TYPE.SOLR){
			return null;
		}
		else
			return null;
	}
	
	public WriterFlowSocket getDestinationWriter(String instanceName) {
		if (!destinationWriterMap.containsKey(instanceName)){
			WarehouseNosqlParam param = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(GlobalParam.nodeTreeConfigs.getConfigMap().get(instanceName).getTransParam().getWriteTo());
			if (param == null)
				return null;
			destinationWriterMap.put(instanceName, WriterFactory.getWriter(param));
		}    
		return destinationWriterMap.get(instanceName);
	}
	
	private SearcherFlowSocket getIndexSearcher(String secname) {
		if (searcherMap.containsKey(secname))
			return searcherMap.get(secname);
		String destination = GlobalParam.nodeTreeConfigs.getSearchConfigs().get(secname).getTransParam().getSearcher(); 
		WarehouseParam param;
		if(GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(destination)!=null){
			param = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(destination);
		}else{
			param = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(destination);
		}
		if (param == null)
			return null;
		
		NodeConfig paramConfig = GlobalParam.nodeTreeConfigs.getSearchConfigs().get(secname);
		SearcherFlowSocket searcher = FNSearcherSocketFactory.getSearcherInstance(param, paramConfig);
		searcherMap.put(secname, searcher); 
		return searcher;
	}
	
}
