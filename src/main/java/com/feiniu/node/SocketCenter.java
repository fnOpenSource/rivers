package com.feiniu.node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.instruction.flow.TransDataFlow;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.reader.ReaderFlowSocketFactory;
import com.feiniu.searcher.Searcher;
import com.feiniu.searcher.SearcherFlowSocket;
import com.feiniu.searcher.SearcherSocketFactory;
import com.feiniu.util.Common;
import com.feiniu.writer.WriterFlowSocket;
import com.feiniu.writer.WriterSocketFactory;

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
	private Map<String, Searcher> searcherMap = new ConcurrentHashMap<String, Searcher>();
	
	private Map<String, WriterFlowSocket> writerSocketMap = new ConcurrentHashMap<String, WriterFlowSocket>();
	private Map<String, ReaderFlowSocket<?>> readerSocketMap = new ConcurrentHashMap<String, ReaderFlowSocket<?>>();
	private Map<String, SearcherFlowSocket> searcherSocketMap = new ConcurrentHashMap<String, SearcherFlowSocket>(); 
 
	public Searcher getSearcher(String instance, String seq,String tag,boolean reload) {
		synchronized (searcherMap) {
			if (reload || !searcherMap.containsKey(instance)) {
				if (!GlobalParam.nodeConfig.getSearchConfigs().containsKey(instance))
					return null;
				InstanceConfig instanceConfig = GlobalParam.nodeConfig.getSearchConfigs().get(instance);
				Searcher searcher = Searcher.getInstance(instance, instanceConfig, getSearcherSocket(instance,seq,tag));
				searcherMap.put(instance, searcher);
			}
			return searcherMap.get(instance);
		} 
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
		synchronized (transDataFlowMap) {
			String tags = Common.getResourceTag(instance, seq,tag,false);
			if (!transDataFlowMap.containsKey(tags) || needClear) { 
				TransDataFlow transDataFlow  = TransDataFlow.getInstance(getReaderSocket(instance, seq,tag), getWriterSocket(instance, seq,tag),
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance));
				transDataFlowMap.put(tags, transDataFlow);
			}
			return transDataFlowMap.get(tags);
		} 
	} 

	public ReaderFlowSocket<?> getReaderSocket(String instance, String seq,String tag) {
		synchronized (readerSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if(GlobalParam.nodeConfig.getInstanceConfigs().get(instance)!=null)
				ignoreSeqUseAlias = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().isReaderPoolShareAlias();
			String tagInstance = instance;
			if(ignoreSeqUseAlias)
				tagInstance = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, seq,tag,ignoreSeqUseAlias);
			
			if (!readerSocketMap.containsKey(tags)) {  
				WarehouseParam param = getWHP(
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().getDataFrom());
				if (param == null)
					return null;  
				readerSocketMap.put(tags, ReaderFlowSocketFactory.getInstance(param, seq,GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().getDataFromhandler()));
			}
			return readerSocketMap.get(tags);
		} 
	}

	public WriterFlowSocket getWriterSocket(String instance, String seq,String tag) {
		synchronized (writerSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if(GlobalParam.nodeConfig.getInstanceConfigs().get(instance)!=null)
				ignoreSeqUseAlias = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().isWriterPoolShareAlias();
			String tagInstance = instance;
			if(ignoreSeqUseAlias)
				tagInstance = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, seq,tag,ignoreSeqUseAlias);
			
			if (!writerSocketMap.containsKey(tags)) {
				WarehouseParam param = getWHP(
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().getWriteTo());
				if (param == null)
					return null;
				writerSocketMap.put(tags, WriterSocketFactory.getInstance(param, seq,GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().getWriteTohandler()));
			}
			return writerSocketMap.get(tags);
		} 
	}

	private SearcherFlowSocket getSearcherSocket(String instance, String seq,String tag) {
		synchronized (searcherSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if(GlobalParam.nodeConfig.getInstanceConfigs().get(instance)!=null)
				ignoreSeqUseAlias = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParam().isSearcherShareAlias();
			String tagInstance = instance;
			if(ignoreSeqUseAlias)
				tagInstance = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, seq,tag,ignoreSeqUseAlias);
			
			if (!searcherSocketMap.containsKey(tags)) {
				WarehouseParam param = getWHP(
						GlobalParam.nodeConfig.getSearchConfigs().get(instance).getPipeParam().getSearcher());
				if (param == null)
					return null;

				InstanceConfig paramConfig = GlobalParam.nodeConfig.getSearchConfigs().get(instance);
				SearcherFlowSocket searcher = SearcherSocketFactory.getInstance(param, paramConfig, null);
				searcherSocketMap.put(tags, searcher);
			}
			return searcherSocketMap.get(tags); 
		} 
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

}
