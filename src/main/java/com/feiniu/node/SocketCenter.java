package com.feiniu.node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.feiniu.computer.Computer;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.param.warehouse.WarehouseParam;
import com.feiniu.piper.TransDataFlow;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.reader.ReaderFlowSocketFactory;
import com.feiniu.searcher.Searcher;
import com.feiniu.searcher.SearcherFlowSocket;
import com.feiniu.searcher.SearcherSocketFactory;
import com.feiniu.util.Common;
import com.feiniu.writer.WriterFlowSocket;
import com.feiniu.writer.WriterSocketFactory;

/**
 * data-flow router reader searcher computer and writer control center seq only
 * support for reader to read series data source and create one or more instance
 * in writer searcherMap and computerMap for data to user data transfer
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 13:55
 */
public final class SocketCenter {

	/** for special data transfer **/
	private Map<String, Searcher> searcherMap = new ConcurrentHashMap<>();
	private Map<String, Computer> computerMap = new ConcurrentHashMap<>();

	/** for normal transfer **/
	private Map<String, TransDataFlow> transDataFlowMap = new ConcurrentHashMap<>();
	private Map<String, WriterFlowSocket> writerSocketMap = new ConcurrentHashMap<>();
	private Map<String, ReaderFlowSocket> readerSocketMap = new ConcurrentHashMap<>();
	private Map<String, SearcherFlowSocket> searcherSocketMap = new ConcurrentHashMap<>();

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
			String tags = Common.getResourceTag(instance, seq, tag, false);
			if (!transDataFlowMap.containsKey(tags) || needClear) {
				TransDataFlow transDataFlow = TransDataFlow.getInstance(
						getReaderSocket(
								GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadFrom(),
								instance, seq, tag),
						getWriterSocket(
								GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(),
								instance, seq, tag),
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance));
				transDataFlowMap.put(tags, transDataFlow);
			}
			return transDataFlowMap.get(tags);
		}
	}

	public Computer getComputer(String instance, String seq, String tag, boolean reload) {
		synchronized (computerMap) {
			if (reload || !computerMap.containsKey(instance)) {
				if (!GlobalParam.nodeConfig.getInstanceConfigs().containsKey(instance))
					return null;
				InstanceConfig instanceConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(instance);
				Computer computer = Computer.getInstance(instance, instanceConfig,
						getReaderSocket(
								GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getModelFrom(),
								instance, seq, tag));
				computerMap.put(instance, computer);
			}
		}
		return computerMap.get(instance);
	}

	public Searcher getSearcher(String instance, String seq, String tag, boolean reload) {
		synchronized (searcherMap) {
			if (reload || !searcherMap.containsKey(instance)) {
				if (!GlobalParam.nodeConfig.getSearchConfigs().containsKey(instance))
					return null;
				InstanceConfig instanceConfig = GlobalParam.nodeConfig.getSearchConfigs().get(instance);
				Searcher searcher = Searcher.getInstance(instance, instanceConfig,
						getSearcherSocket(
								GlobalParam.nodeConfig.getSearchConfigs().get(instance).getPipeParams().getSearchFrom(),
								instance, seq, tag));
				searcherMap.put(instance, searcher);
			}
			return searcherMap.get(instance);
		}
	}

	public void clearTransDataFlow(String instance, String seq, String tag) {
		synchronized (this) {
			String tags = Common.getResourceTag(instance, seq, tag, false);
			if (transDataFlowMap.containsKey(tags)) {
				transDataFlowMap.remove(tags);

				boolean ignoreSeqUseAlias = false;
				if (GlobalParam.nodeConfig.getInstanceConfigs().get(instance) != null)
					ignoreSeqUseAlias = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
							.isReaderPoolShareAlias();
				String tagInstance = instance;
				if (ignoreSeqUseAlias)
					tagInstance = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getAlias();
				tags = Common.getResourceTag(tagInstance, seq, tag, ignoreSeqUseAlias);
				readerSocketMap.remove(tags);
				writerSocketMap.remove(tags);
			}
		}
	}

	public ReaderFlowSocket getReaderSocket(String resourceName, String instance, String seq, String tag) {
		synchronized (readerSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if (GlobalParam.nodeConfig.getInstanceConfigs().get(instance) != null)
				ignoreSeqUseAlias = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
						.isReaderPoolShareAlias();
			String tagInstance = instance;
			if (ignoreSeqUseAlias)
				tagInstance = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, seq, tag, ignoreSeqUseAlias);

			if (!readerSocketMap.containsKey(tags)) {
				WarehouseParam param = getWHP(resourceName);
				if (param == null)
					return null;
				readerSocketMap.put(tags, ReaderFlowSocketFactory.getInstance(param, seq,
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadHandler()));
			}
			return readerSocketMap.get(tags);
		}
	}

	public WriterFlowSocket getWriterSocket(String resourceName, String instance, String seq, String tag) {
		synchronized (writerSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if (GlobalParam.nodeConfig.getInstanceConfigs().get(instance) != null)
				ignoreSeqUseAlias = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
						.isWriterPoolShareAlias();
			String tagInstance = instance;
			if (ignoreSeqUseAlias)
				tagInstance = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, seq, tag, ignoreSeqUseAlias);

			if (!writerSocketMap.containsKey(tags)) {
				WarehouseParam param = getWHP(resourceName);
				if (param == null)
					return null;
				writerSocketMap.put(tags, WriterSocketFactory.getInstance(param, seq,
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteHandler()));
			}
			return writerSocketMap.get(tags);
		}
	}

	public SearcherFlowSocket getSearcherSocket(String resourceName, String instance, String seq, String tag) {
		synchronized (searcherSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if (GlobalParam.nodeConfig.getInstanceConfigs().get(instance) != null)
				ignoreSeqUseAlias = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
						.isSearcherShareAlias();
			String tagInstance = instance;
			if (ignoreSeqUseAlias)
				tagInstance = GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, seq, tag, ignoreSeqUseAlias);

			if (!searcherSocketMap.containsKey(tags)) {
				WarehouseParam param = getWHP(resourceName);
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
		if (GlobalParam.nodeConfig.getNoSqlWarehouse().containsKey(destination)) {
			param = GlobalParam.nodeConfig.getNoSqlWarehouse().get(destination);
		} else if (GlobalParam.nodeConfig.getSqlWarehouse().containsKey(destination)) {
			param = GlobalParam.nodeConfig.getSqlWarehouse().get(destination);
		}
		return param;
	}
}
