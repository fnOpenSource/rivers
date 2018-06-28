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
import com.feiniu.config.NodeConfig;
import com.feiniu.connect.ESConnector;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.PipeDataUnit;
import com.feiniu.model.param.TransParam;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;

/**
 * ElasticSearch Writer Manager
 * 
 * @author chengwen
 * @version 1.0
 */
@NotThreadSafe
public class ESFlow extends WriterFlowSocket {

	private ESConnector ESC;
	private final static Logger log = LoggerFactory.getLogger("ESFlow");

	public static ESFlow getInstance(HashMap<String, Object> connectParams) {
		ESFlow o = new ESFlow();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public void getResource() {
		synchronized (this) {
			boolean renew = false;
			if(this.ESC==null) {
				retainer.set(0);
				renew = true;
			}
			if(retainer.incrementAndGet() == 1)
				renew = true;
			if (renew) {
				LINK(false);
				this.ESC = (ESConnector) this.FC.getConnection(false);
			} 
		}
	}

	@Override
	public void freeResource(boolean releaseConn) { 
		synchronized (this) {  
			if (retainer.decrementAndGet() == 0) {
				this.ESC = null;
				UNLINK(this.FC, releaseConn);
			}else {
				log.info(this.ESC.toString() + " retainer is " + retainer.get());
			}
		} 
	}
	
	@Override
	public void MONOPOLY() {  
		synchronized (this) {
			if(this.ESC==null) { 
				LINK(false); 
				retainer.set(1);
				this.ESC = (ESConnector) this.FC.getConnection(false);
			}
		}
	} 

	@Override
	public void write(String keyColumn, PipeDataUnit unit, Map<String, TransParam> transParams, String instantcName,
			String storeId, boolean isUpdate) throws FNException {
		try {
			String name = Common.getStoreName(instantcName, storeId);
			String type = instantcName;
			if (unit==null || unit.getData().size() == 0) {
				log.info(instantcName+" WriteUnit contain Dirty data!");
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
					if (transParam.getSeparator() != null) {
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
					this.ESC.getBulkProcessor().add(_UR); 
				} else {
					this.ESC.getClient().update(_UR).get();
				}
			} else {
				IndexRequestBuilder _IB = this.ESC.getClient().prepareIndex(name, type, id);
				_IB.setSource(cbuilder);
				if (routing.length() > 0)
					_IB.setRouting(routing.toString());
				if (this.isBatch) {
					this.ESC.getBulkProcessor().add(_IB.request()); 
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
	public void doDelete(SearcherModel<?, ?, ?> query, String instance, String storeId) throws Exception {
 
	}

	@Override
	public void flush() throws Exception {
		if (this.isBatch) {
			this.ESC.getBulkProcessor().flush();
			if (this.ESC.getRunState() == false) {
				this.ESC.setRunState(true);
				throw new FNException("BulkProcessor Exception!Need Redo!");
			}
		}
	}

	/**
	 * add index
	 * 
	 * @param seq
	 *            for series data source sequence
	 * @param instanceName
	 *            data source main tag name
	 */
	@Override
	public boolean settings(String instanceName, String storeId, Map<String, TransParam> transParams) {
		String name = Common.getStoreName(instanceName, storeId);
		String type = instanceName;
		try {
			log.info("setting index " + name + ":" + type);
			IndicesExistsResponse indicesExistsResponse = this.ESC.getClient().admin().indices()
					.exists(new IndicesExistsRequest(name)).actionGet();
			if (!indicesExistsResponse.isExists()) {
				CreateIndexResponse createIndexResponse = this.ESC.getClient().admin().indices()
						.create(new CreateIndexRequest(name)).actionGet();
				log.info("create new index " + name + " response isAcknowledged:"
						+ createIndexResponse.isAcknowledged());
			}

			PutMappingRequest mappingRequest = new PutMappingRequest(name).type(type);
			mappingRequest.source(getSettingMap(transParams));
			PutMappingResponse response = this.ESC.getClient().admin().indices().putMapping(mappingRequest).actionGet();
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

			ForceMergeResponse response = this.ESC.getClient().admin().indices().forceMerge(request).actionGet();
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
	public void remove(String instanceName, String storeId) {
		if (storeId == null || storeId.length() == 0)
			return;
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to remove index " + name);
			IndicesExistsResponse res = this.ESC.getClient().admin().indices().prepareExists(name).execute()
					.actionGet();
			if (!res.isExists()) {
				log.info("index " + name + " didn't exist.");
			} else {
				DeleteIndexRequest deleteRequest = new DeleteIndexRequest(name);
				DeleteIndexResponse deleteResponse = this.ESC.getClient().admin().indices().delete(deleteRequest)
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
	public String getNewStoreId(String instance, boolean isIncrement, String dbseq, NodeConfig nodeConfig) {
		String instanceName = Common.getInstanceName(instance, dbseq, nodeConfig.getPipeParam().getInstanceName());
		boolean a_alias = false;
		boolean b_alias = false;
		boolean a = this.ESC.getClient().admin().indices()
				.exists(new IndicesExistsRequest(Common.getStoreName(instanceName, "a"))).actionGet().isExists();
		if (a)
			a_alias = getIndexAlias(instanceName, "a", nodeConfig.getAlias());
		boolean b = this.ESC.getClient().admin().indices()
				.exists(new IndicesExistsRequest(Common.getStoreName(instanceName, "b"))).actionGet().isExists();
		if (b)
			b_alias = getIndexAlias(instanceName, "b", nodeConfig.getAlias());
		String select = "";
		if (isIncrement) {
			if (a && b) {
				if (a_alias) {
					if (b_alias) {
						if (getDocumentNums(instanceName, "a") > this.getDocumentNums(instanceName, "b")) {
							this.ESC.getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(instanceName, "b")));
							select = "a";
						} else {
							this.ESC.getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(instanceName, "a")));
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
				this.settings(instanceName, select, nodeConfig.getTransParams());
			}

			if ((select.equals("a") && !a) || (select.equals("b") && !b)
					|| !this.getIndexAlias(instanceName, select, nodeConfig.getAlias())) {
				setAlias(instanceName, select, nodeConfig.getAlias());
			}
		} else {
			if (a && b) {
				if (a_alias) {
					if (b_alias) {
						if (getDocumentNums(instanceName, "a") > getDocumentNums(instanceName, "b")) {
							this.ESC.getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(instanceName, "b")));
							select = "b";
						} else {
							this.ESC.getClient().admin().indices()
									.delete(new DeleteIndexRequest(Common.getStoreName(instanceName, "a")));
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
			IndicesAliasesResponse response = this.ESC.getClient().admin().indices().prepareAliases().addAlias(name,
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
		config_map.put(GlobalParam.DEFAULT_FIELD, new HashMap<String, Object>(){{put("type", "long");}});
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
		IndicesStatsResponse response = this.ESC.getClient().admin().indices().prepareStats(name).all().get();
		long res = response.getPrimaries().getDocs().getCount();
		return res;
	}

	private boolean getIndexAlias(String instanceName, String storeId, String alias) {
		String name = Common.getStoreName(instanceName, storeId);
		AliasesExistResponse response = this.ESC.getClient().admin().indices().prepareAliasesExist(alias)
				.setIndices(name).get();
		return response.exists();
	}
}
