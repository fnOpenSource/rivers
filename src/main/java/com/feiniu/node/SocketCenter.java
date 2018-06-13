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
import com.feiniu.reader.flow.ReaderFlowSocket;
import com.feiniu.reader.flow.ReaderFlowSocketFactory;
import com.feiniu.searcher.FNQueryBuilder;
import com.feiniu.searcher.FNSearcher;
import com.feiniu.searcher.FNSearcherSocketFactory;
import com.feiniu.searcher.flow.SearcherFlowSocket;
import com.feiniu.util.Common;
import com.feiniu.writer.WriterFactory;
import com.feiniu.writer.flow.JobWriter;
import com.feiniu.writer.flow.WriterFlowSocket;

/**
 * data-flow router reader searcher and writer control center
 * seq only support for reader to read series data source,and create 
 * one or more instance in writer single destination.
 * @author chengwen
 * @version 1.0 
 */
public final class SocketCenter{  
	 
	private Map<String,JobWriter> writerChannelMap = new ConcurrentHashMap<String, JobWriter>();
	private Map<String, WriterFlowSocket> destinationWriterMap = new ConcurrentHashMap<String, WriterFlowSocket>();
	private Map<String, SearcherFlowSocket> searcherFlowMap = new ConcurrentHashMap<String, SearcherFlowSocket>(); 
	private Map<String, FNSearcher> searcherMap = new ConcurrentHashMap<String, FNSearcher>();
	
	private final static Logger log = LoggerFactory.getLogger(SocketCenter.class);  
	  
	
	public FNSearcher getSearcher(String instanceName) { 
		if(!searcherMap.containsKey(instanceName)) {
			if (!GlobalParam.nodeTreeConfigs.getSearchConfigs().containsKey(instanceName))
				return null; 
			NodeConfig paramConfig = GlobalParam.nodeTreeConfigs.getSearchConfigs().get(instanceName);
			FNSearcher searcher = FNSearcher.getInstance(instanceName, paramConfig, getSearcherFlow(instanceName));
			searcherMap.put(instanceName, searcher);
		}
		return searcherMap.get(instanceName);
	}
	
	/**
	 * get Writer auto look for sql and nosql maps
	 * @param seq for series data source sequence
	 * @param instanceName data source main tag name
	 * @param needClear for reset resource
	 * @param tag  Marking resource
	 */ 
	public JobWriter getWriterChannel(String instanceName, String seq,boolean needClear,String tag) { 
		NodeConfig paramConfig = GlobalParam.nodeTreeConfigs.getNodeConfigs().get(instanceName);
		if(!writerChannelMap.containsKey(Common.getInstanceName(instanceName, seq,null)+tag) || needClear){ 
			JobWriter writer=null;
			String readFrom = paramConfig.getTransParam().getDataFrom(); 
			ReaderFlowSocket<?> flowSocket;
			if(GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(readFrom)!=null){ 
				Map<String, WarehouseNosqlParam> dataMap = GlobalParam.nodeTreeConfigs.getNoSqlParamMap();
				if (!dataMap.containsKey(readFrom)){
					log.error("data source config " + readFrom + " not exists");
					return null;
				}   
				flowSocket = ReaderFlowSocketFactory.getChannel(dataMap.get(readFrom),seq);
			}else{ 
				Map<String, WarehouseSqlParam> dataMap = GlobalParam.nodeTreeConfigs.getSqlParamMap();
				if (!dataMap.containsKey(readFrom)){
					log.error("data source config " + readFrom + " not exists");
					return null;
				}   
				flowSocket = ReaderFlowSocketFactory.getChannel(dataMap.get(readFrom),seq);
			} 
			writer = JobWriter.getInstance(flowSocket,getDestinationWriter(instanceName,seq), paramConfig);
			writerChannelMap.put(Common.getInstanceName(instanceName, seq,null), writer);
		}
		return writerChannelMap.get(Common.getInstanceName(instanceName, seq,null)); 
	}  
 
	public FNQueryBuilder getQueryBuilder(String instanceName) { 
		if (!GlobalParam.nodeTreeConfigs.getNodeConfigs().containsKey(instanceName))
			return null;
		
		WarehouseNosqlParam param = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(GlobalParam.nodeTreeConfigs.getNodeConfigs().get(instanceName).getTransParam().getWriteTo());
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
	
	public WarehouseParam getWHP(String destination) {
		WarehouseParam param=null;
		if(GlobalParam.nodeTreeConfigs.getNoSqlParamMap().containsKey(destination)) {
			param = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(destination);
		}else if(GlobalParam.nodeTreeConfigs.getSqlParamMap().containsKey(destination)) {
			param = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(destination);
		}
		return param;
	}
	
	public WriterFlowSocket getDestinationWriter(String instanceName,String seq) {
		if (!destinationWriterMap.containsKey(instanceName)){
			WarehouseParam param = getWHP(GlobalParam.nodeTreeConfigs.getNodeConfigs().get(instanceName).getTransParam().getWriteTo()); 
			if (param == null)
				return null;
			destinationWriterMap.put(instanceName, WriterFactory.getWriter(param,seq));
		}    
		return destinationWriterMap.get(instanceName);
	}
	
	private SearcherFlowSocket getSearcherFlow(String secname) {
		if (searcherFlowMap.containsKey(secname))
			return searcherFlowMap.get(secname); 
		WarehouseParam param = getWHP(GlobalParam.nodeTreeConfigs.getSearchConfigs().get(secname).getTransParam().getSearcher());
		if (param == null)
			return null;
		
		NodeConfig paramConfig = GlobalParam.nodeTreeConfigs.getSearchConfigs().get(secname);
		SearcherFlowSocket searcher = FNSearcherSocketFactory.getSearcherFlow(param, paramConfig,null);
		searcherFlowMap.put(secname, searcher); 
		return searcher;
	}
	
}
