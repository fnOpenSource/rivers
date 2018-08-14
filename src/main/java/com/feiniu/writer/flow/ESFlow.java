package com.feiniu.writer.flow;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.connect.ESConnector;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.model.PipeDataUnit;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.param.TransParam;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.writer.WriterFlowSocket;

/**
 * ElasticSearch Writer Manager
 * 
 * @author chengwen
 * @version 1.0
 */
@NotThreadSafe
public class ESFlow extends WriterFlowSocket {
	
	protected ESConnector CONNS;
 
	private final static Logger log = LoggerFactory.getLogger("ESFlow");

	public static ESFlow getInstance(HashMap<String, Object> connectParams) {
		ESFlow o = new ESFlow();
		o.INIT(connectParams);
		return o;
	} 

	@Override
	public void write(String keyColumn, PipeDataUnit unit, Map<String, TransParam> transParams, String instance,
			String storeId, boolean isUpdate) throws FNException {
		try {
			String name = Common.getStoreName(instance, storeId);
			String type = instance;
			if (unit==null || unit.getData().size() == 0) {
				log.info(instance+" WriteUnit contain Dirty data!");
				return;
			}
			String id = unit.getKeyColumnVal();
			XContentBuilder cbuilder = jsonBuilder().startObject();
			StringBuilder routing = new StringBuilder();
			for (Entry<String, Object> r : unit.getData().entrySet()) {
				String field = r.getKey();
				if (r.getValue() == null)
					continue;
				String value = String.valueOf(r.getValue());
				TransParam transParam = transParams.get(field);
				if (transParam == null)
					transParam = transParams.get(field.toLowerCase());
				if (transParam == null)
					continue;
				
				if (transParam.getAnalyzer().equalsIgnoreCase(("not_analyzed"))) {
					if(transParam.getIndextype().equalsIgnoreCase("geo_point")) {
						String[] vs = value.split(transParam.getSeparator());
						if(vs.length==2)
							cbuilder.latlon(field, Double.parseDouble(vs[0]), Double.parseDouble(vs[1]));
					}else if (transParam.getSeparator() != null) {
						String[] vs = value.split(transParam.getSeparator());
						cbuilder.array(field, vs);
					} else 
						cbuilder.field(field, value);
				} else {
					cbuilder.field(field, value);
				}
				if (transParam.isRouter()) {
					routing.append(value);
				}
			}
			cbuilder.field(GlobalParam.DEFAULT_FIELD, unit.getUpdateTime());
			cbuilder.endObject();
			if (isUpdate) {
				UpdateRequest _UR = new UpdateRequest(name, type, id);
				_UR.doc(cbuilder).upsert(cbuilder);
				if (routing.length() > 0)
					_UR.routing(routing.toString());
				if (this.isBatch) {
					getESC().getBulkProcessor().add(_UR); 
				} else {
					getESC().getClient().update(_UR).get();
				}
			} else {
				IndexRequestBuilder _IB = getESC().getClient().prepareIndex(name, type, id);
				_IB.setSource(cbuilder);
				if (routing.length() > 0)
					_IB.setRouting(routing.toString());
				if (this.isBatch) {
					getESC().getBulkProcessor().add(_IB.request()); 
				} else {
					_IB.execute().actionGet();
				}
			} 
		} catch (Exception e) {
			log.error("write Exception", e);
			if (e.getMessage().contains("IndexNotFoundException")) {
				throw new FNException("storeId not found");
			} else {
				throw new FNException(e.getMessage());
			}
		}
	}

	@Override
	public void delete(SearcherModel<?, ?, ?> query, String instance, String storeId) throws Exception {
		 
	}

	@Override
	public void flush() throws Exception {
		if (this.isBatch) {
			getESC().getBulkProcessor().flush();
			if (getESC().getRunState() == false) {
				getESC().setRunState(true);
				throw new FNException("BulkProcessor Exception!Need Redo!");
			}
		}
	}

	/**
	 * add index
	 * 
	 * @param seq
	 *            for series data source sequence
	 * @param instance
	 *            data source main tag name
	 */
	@Override
	public boolean create(String instance, String storeId, Map<String, TransParam> transParams) {
		String name = Common.getStoreName(instance, storeId);
		String type = instance;
		try {
			log.info("setting index " + name + ":" + type);
			IndicesExistsResponse indicesExistsResponse = getESC().getClient().admin().indices()
					.exists(new IndicesExistsRequest(name)).actionGet();
			if (!indicesExistsResponse.isExists()) {
				CreateIndexResponse createIndexResponse = getESC().getClient().admin().indices()
						.create(new CreateIndexRequest(name)).actionGet();
				log.info("create new index " + name + " response isAcknowledged:"
						+ createIndexResponse.isAcknowledged());
			}

			PutMappingRequest mappingRequest = new PutMappingRequest(name).type(type);
			mappingRequest.source(getSettingMap(transParams));
			PutMappingResponse response = getESC().getClient().admin().indices().putMapping(mappingRequest).actionGet();
			log.info("setting response isAcknowledged:" + response.isAcknowledged());
			return true;
		} catch (Exception e) {
			log.error("setting index " + name + ":" + type + " failed.", e);
			return false;
		}
	}

	@Override
	public void optimize(String instanceName, String storeId) {
		String name = Common.getStoreName(instanceName, storeId);
		try {
			ForceMergeRequest request = new ForceMergeRequest(name);
			request.maxNumSegments(2);
			request.flush(true);
			request.onlyExpungeDeletes(true);

			ForceMergeResponse response = getESC().getClient().admin().indices().forceMerge(request).actionGet();
			int failed_cnt = response.getFailedShards();
			if (failed_cnt > 0) {
				log.warn("index " + name + " optimize getFailedShards " + failed_cnt);
			} else {
				log.info("index " + name + " optimize Success!");
			}
		} catch (Exception e) {
			log.error("index " + name + " optimize failed.", e);
		}
	}

	@Override
	public void removeInstance(String instanceName, String storeId) {
		if (storeId == null || storeId.length() == 0)
			return;
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to remove index " + name);
			IndicesExistsResponse res = getESC().getClient().admin().indices().prepareExists(name).execute()
					.actionGet();
			if (!res.isExists()) {
				log.info("index " + name + " didn't exist.");
			} else {
				DeleteIndexRequest deleteRequest = new DeleteIndexRequest(name);
				DeleteIndexResponse deleteResponse = getESC().getClient().admin().indices().delete(deleteRequest)
						.actionGet();
				if (deleteResponse.isAcknowledged()) {
					log.info("index " + name + " removed ");
				}
			}
		} catch (Exception e) {
			log.error("remove index " + name + " Exception", e);
		}
	}

	@Override
	public String getNewStoreId(String mainName, boolean isIncrement, InstanceConfig instanceConfig) { 
		boolean a_alias = false;
		boolean b_alias = false;
		boolean a = getESC().getClient().admin().indices()
				.exists(new IndicesExistsRequest(Common.getStoreName(mainName, "a"))).actionGet().isExists();
		if (a)
			a_alias = getIndexAlias(mainName, "a", instanceConfig.getAlias());
		boolean b = getESC().getClient().admin().indices()
				.exists(new IndicesExistsRequest(Common.getStoreName(mainName, "b"))).actionGet().isExists();
		if (b)
			b_alias = getIndexAlias(mainName, "b", instanceConfig.getAlias());
		String select = "";
		if (isIncrement) {
			if (a && b) {
				if (a_alias) {
					if (b_alias) {
						if (getDocumentNums(mainName, "a") > this.getDocumentNums(mainName, "b")) {
							getESC().getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(mainName, "b")));
							select = "a";
						} else {
							getESC().getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(mainName, "a")));
							select = "b";
						}
					} else {
						select = "a";
					}
				} else {
					select = "b";
				}
			} else {
				select = a ? "a" : (b ? "b" : "a");
			}

			if ((select.equals("a") && !a) || (select.equals("b") && !b)) {
				this.create(mainName, select, instanceConfig.getTransParams());
			}

			if ((select.equals("a") && !a) || (select.equals("b") && !b)
					|| !this.getIndexAlias(mainName, select, instanceConfig.getAlias())) {
				setAlias(mainName, select, instanceConfig.getAlias());
			}
		} else {
			if (a && b) {
				if (a_alias) {
					if (b_alias) {
						if (getDocumentNums(mainName, "a") > getDocumentNums(mainName, "b")) {
							getESC().getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(mainName, "b")));
							select = "b";
						} else {
							getESC().getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(mainName, "a")));
							select = "a";
						}
					} else {
						select = "b";
					}
				}
				select = "a";
			} else {
				select = "b";
				if (b && b_alias) {
					select = "a";
				}
			}
		}
		return select;
	}

	@Override
	public void setAlias(String instanceName, String storeId, String aliasName) {
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to setting Alias " + aliasName + " to index " + name);
			IndicesAliasesResponse response = getESC().getClient().admin().indices().prepareAliases().addAlias(name,
					aliasName).execute().actionGet();
			if (response.isAcknowledged()) {
				log.info("alias " + aliasName + " setted to " + name);
			}
		} catch (Exception e) {
			log.error("alias " + aliasName + " set to " + name + " Exception.", e);
		}
	} 
	
	private Map<String, Object> getSettingMap(Map<String, TransParam> transParams) {
		Map<String, Object> config_map = new HashMap<String, Object>();
		try {

			for (Map.Entry<String, TransParam> e : transParams.entrySet()) {
				Map<String, Object> map = new HashMap<String, Object>();
				TransParam p = e.getValue();
				if (p.getName() == null)
					continue;
				map.put("type", p.getIndextype()); // type is must
				if (p.getStored().toLowerCase().equals("false")) {
					map.put("store", "false");
				}
				if (p.getIndexed().toLowerCase().equals("true")) {
					if (p.getAnalyzer().length() > 0) {
						if (p.getAnalyzer().equalsIgnoreCase(("not_analyzed")))
							map.put("index", "not_analyzed");
						else {
							map.put("index", "analyzed");
							map.put("analyzer", p.getAnalyzer());
							Map<String, Boolean> enabledMap = new HashMap<String, Boolean>();
							enabledMap.put("enabled", false);
							map.put("norms", enabledMap);
							map.put("index_options", "docs");
						}
					}
				} else {
					map.put("index", "not_analyzed");
				}
				config_map.put(p.getAlias(), map);
			}
		} catch (Exception e) {
			log.error("getSettingMap error:" + e.getMessage());
		}
		config_map.put(GlobalParam.DEFAULT_FIELD, new HashMap<String, Object>(){
			private static final long serialVersionUID = 1L;{put("type", "long");}});
		Map<String, Object> root_map = new HashMap<String, Object>();
		root_map.put("properties", config_map);
		Map<String, Object> _source_map = new HashMap<String, Object>();
		_source_map.put("enabled", "true");
		root_map.put("_source", _source_map);
		Map<String, Object> _all_map = new HashMap<String, Object>();
		_all_map.put("enabled", "false");
		root_map.put("_all", _all_map);
		return root_map;
	}

	private long getDocumentNums(String instanceName, String storeId) {
		String name = Common.getStoreName(instanceName, storeId);
		IndicesStatsResponse response = getESC().getClient().admin().indices().prepareStats(name).all().get();
		long res = response.getPrimaries().getDocs().getCount();
		return res;
	}

	private boolean getIndexAlias(String instanceName, String storeId, String alias) {
		String name = Common.getStoreName(instanceName, storeId);
		AliasesExistResponse response = getESC().getClient().admin().indices().prepareAliasesExist(alias)
				.setIndices(name).get();
		return response.exists();
	}
	
	public void REALEASE(boolean isMonopoly,boolean releaseConn){
		if(isMonopoly==false) { 
			synchronized(retainer){ 
				if(retainer.decrementAndGet()<=0){
					FnConnectionPool.freeConn(this.FC, this.poolName,releaseConn);
					this.CONNS = null;
					retainer.set(0); 
				}else{
					log.info(this.FC+" retainer is "+retainer.get());
				}
			} 
		} 
	}
	
	private ESConnector getESC() { 
		synchronized (this) {
			if(this.CONNS==null)
				this.CONNS = (ESConnector) GETSOCKET().getConnection(false); 
			return this.CONNS;
		}
	}
}
