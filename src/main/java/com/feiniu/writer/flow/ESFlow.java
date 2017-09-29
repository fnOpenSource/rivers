package com.feiniu.writer.flow;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
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
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.NodeConfig;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;
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
	private final static int BULK_BUFFER = 1000;
	private final static int BULK_SIZE = 30;
	private final static int BULK_FLUSH = 3;
	private final static int BULK_CONCURRENT = 1; 
	
	private Client conn;  
	private BulkProcessor bulkProcessor;
	private final static Logger log = LoggerFactory.getLogger(ESFlow.class);

	public static ESFlow getInstance(HashMap<String, Object> connectParams) {
		ESFlow o = new ESFlow();
		o.INIT(connectParams);
		return o;
	} 

	@Override
	public void getResource() {
		while (locked.get()) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				log.error("getResource Exception", e);
			}
		}
		locked.set(true);
		PULL(false);
		this.conn = (Client) this.FC.getConnection();
	}
 
	@Override
	public void write(WriteUnit unit,Map<String, WriteParam> writeParamMap, String instantcName, String storeId,boolean isUpdate)
			throws FNException {
		try {
			if (this.batch && this.bulkProcessor==null) {
				this.bulkProcessor = getBulkProcessor(this.conn);
			}
			String name = Common.getStoreName(instantcName, storeId);
			String type = instantcName;
			if (unit.getData().size() == 0) {
				log.info("Empty IndexUnit for " + name + " " + type);
				return;
			} 
			String id = unit.getKeyColumnVal(); 
			XContentBuilder cbuilder = jsonBuilder().startObject();
			
			for(Entry<String, Object> r:unit.getData().entrySet()){
				String field = r.getKey(); 
				if (r.getValue() == null)
					continue;
				String value = String.valueOf(r.getValue());
				WriteParam writeParam = writeParamMap.get(field);
				if (writeParam == null)
					writeParam = writeParamMap.get(field.toLowerCase());
				if (writeParam == null)
					writeParam = writeParamMap.get(field.toUpperCase());
				if (writeParam == null)
					continue;

				if (writeParam.getAnalyzer().equalsIgnoreCase(("not_analyzed"))) {
					if (writeParam.getSeparator() != null) {
						String[] vs = value.split(writeParam.getSeparator()); 
						cbuilder.array(field, vs);
					} else
						cbuilder.field(field, value);
				} else {
					cbuilder.field(field, value.toLowerCase());
				}
			} 
			cbuilder.field("SYSTEM_UPDATE_TIME", unit.getUpdateTime());
			cbuilder.endObject();
			if(isUpdate){
				UpdateRequest _UR = new UpdateRequest(name, type, id);
				_UR.doc(cbuilder);
				if (!batch) {
					this.conn.update(_UR);
				} else {
					this.bulkProcessor.add(_UR);
				}
			}else{
				IndexRequestBuilder _IB = this.conn
						.prepareIndex(name, type, id);
				_IB.setSource(cbuilder);
				if (!batch) {
					_IB.execute().actionGet();
				} else {
					this.bulkProcessor.add(_IB.request());
				}
			}  
		} catch (Exception e) {
			if(e.getMessage().contains("IndexNotFoundException")){
				throw new FNException("storeId not found");
			}else{
				throw new FNException(e.getMessage());
			} 
		} 
	}

	@Override
	public void doDelete(WriteUnit unit, String instantcName, String storeId)
			throws Exception {
		String name = Common.getStoreName(instantcName, storeId);
		String type = instantcName;
		Method getMethod = unit.getClass().getMethod("getId", new Class[] {});
		String id = String.valueOf(getMethod.invoke(unit, new Object[] {}));
		log.info("doDelete:" + id + "," + name + "," + type);
		DeleteRequestBuilder builder = conn.prepareDelete(name, type, id);
		if (!builder.execute().actionGet().isFound()) {
			log.info("Delete isFound failed.id=" + id);
		}
	}

	@Override
	public void flush(){
		if (this.batch && this.bulkProcessor!=null) {
			this.bulkProcessor.flush();
		}
	}

	/**
	 * add index
	 * 
	 * @param seq for series data source sequence
	 * @param instanceName data source main tag name
	 */
	@Override
	public boolean settings(String instanceName, String storeId,
			Map<String, WriteParam> paramMap) {
		String name = Common.getStoreName(instanceName, storeId);
		String type = instanceName;
		try {
			log.info("setting index " + name + ":" + type);
			IndicesExistsResponse indicesExistsResponse = this.conn.admin()
					.indices().exists(new IndicesExistsRequest(name))
					.actionGet();
			if (!indicesExistsResponse.isExists()) {
				CreateIndexResponse createIndexResponse = this.conn.admin()
						.indices().create(new CreateIndexRequest(name))
						.actionGet();
				log.info("create new index " + name
						+ " response isAcknowledged:"
						+ createIndexResponse.isAcknowledged());
			}

			PutMappingRequest mappingRequest = new PutMappingRequest(name)
					.type(type);
			mappingRequest.source(getSettingMap(paramMap));
			PutMappingResponse response = this.conn.admin().indices()
					.putMapping(mappingRequest).actionGet();
			log.info("setting response isAcknowledged:"
					+ response.isAcknowledged());
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

			ForceMergeResponse response = this.conn.admin().indices()
					.forceMerge(request).actionGet();
			int failed_cnt = response.getFailedShards();
			if (failed_cnt > 0) {
				log.warn("index " + name + " optimize getFailedShards "
						+ failed_cnt);
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
			IndicesExistsResponse res = this.conn.admin().indices()
					.prepareExists(name).execute().actionGet();
			if (!res.isExists()) {
				log.info("index " + name + " didn't exist.");
			} else {
				DeleteIndexRequest deleteRequest = new DeleteIndexRequest(name);
				DeleteIndexResponse deleteResponse = this.conn.admin()
						.indices().delete(deleteRequest).actionGet();
				if (deleteResponse.isAcknowledged()) {
					log.info("index " + name + " removed ");
				}
			}
		} catch (Exception e) {
			log.error("remove index " + name + " Exception", e);
		}
	}

	@Override
	public String getNewStoreId(String instance, boolean isIncrement,
			String dbseq, NodeConfig nodeConfig) {
		String instanceName = Common.getInstanceName(instance, dbseq);
		boolean a_alias=false;
		boolean b_alias=false;
		boolean a = this.conn
				.admin()
				.indices()
				.exists(new IndicesExistsRequest(
						Common.getStoreName(instanceName, "a"))).actionGet()
				.isExists();
		if(a)
			a_alias = getIndexAlias(instanceName, "a",nodeConfig.getAlias());
		boolean b = this.conn
				.admin()
				.indices()
				.exists(new IndicesExistsRequest(
						Common.getStoreName(instanceName, "b"))).actionGet()
				.isExists();
		if(b)
			b_alias = getIndexAlias(instanceName, "b",nodeConfig.getAlias());
		String select = "";
		if (isIncrement) {
			if (a && b) {
				if (a_alias) {
					if (b_alias) {
						if (getDocumentNums(instanceName, "a") > this
								.getDocumentNums(instanceName, "b")) {
							this.conn
									.admin()
									.indices()
									.delete(new DeleteIndexRequest(
											Common.getStoreName(instanceName, "b")));
							select = "a";
						} else {
							this.conn
									.admin()
									.indices()
									.delete(new DeleteIndexRequest(
											Common.getStoreName(instanceName, "a")));
							select = "b";
						}
					} else {
						select = "a";
					}
				}else{
					select = "b";
				} 
			} else {
				select = a ? "a" : (b ? "b" : "a");
			} 

			if ((select.equals("a") && !a) || (select.equals("b") && !b)) {
				this.settings(instanceName, select,
						nodeConfig.getWriteParamMap());
			}

			if ((select.equals("a") && !a) || (select.equals("b") && !b)
					|| !this.getIndexAlias(instanceName, select,nodeConfig.getAlias())) {
				setAlias(instanceName, select, nodeConfig.getAlias());
			}
		} else {
			if (a && b) {
				if (a_alias) {
					if (b_alias) {
						if (getDocumentNums(instanceName, "a") > getDocumentNums(instanceName, "b")) {
							this.conn
									.admin()
									.indices()
									.delete(new DeleteIndexRequest(
											Common.getStoreName(instanceName, "b")));
							select = "b";
						} else {
							this.conn
									.admin()
									.indices()
									.delete(new DeleteIndexRequest(
											Common.getStoreName(instanceName, "a")));
							select = "a";
						}
					} else {
						select = "b";
					}
				}
				select = "a";
			} else {
				select = "b";
				if(b && b_alias){
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
			log.info("trying to setting Alais " + instanceName + " to index "
					+ name);
			IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
			indicesAliasesRequest.addAlias(aliasName, name);
			IndicesAliasesResponse indicesAliasesResponse = conn.admin()
					.indices().aliases(indicesAliasesRequest).actionGet();
			if (indicesAliasesResponse.isAcknowledged()) {
				log.info("alias " + instanceName + " setted to " + name);
			}
		} catch (Exception e) {
			log.error("alias " + instanceName + " set to " + name
					+ " Exception.", e);
		}
	}
 

	private Map<String, Object> getSettingMap(Map<String, WriteParam> paramMap) {
		Map<String, Object> config_map = new HashMap<String, Object>();
		try {

			for (Map.Entry<String, WriteParam> e : paramMap.entrySet()) {
				Map<String, Object> map = new HashMap<String, Object>();
				WriteParam p = e.getValue();
				if (p.getName() == null)
					continue;
				map.put("type", p.getIndextype()); // type is must
				if (p.getStored().length() > 0) {
					map.put("store", p.getStored());
				}
				if (!"false".equals(p.getIndexed())) {
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
					map.put("index", "no");
				}
				config_map.put(p.getName(), map);
			}
		} catch (Exception e) {
			log.error("getSettingMap error:" + e.getMessage());
		}
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
		IndicesStatsResponse response = this.conn.admin().indices()
				.prepareStats(name).all().get();
		long res = response.getPrimaries().getDocs().getCount();
		return res;
	}

	private boolean getIndexAlias(String instanceName, String storeId,String alias) {
		String name = Common.getStoreName(instanceName, storeId);
		AliasesExistResponse response = this.conn.admin().indices()
				.prepareAliasesExist(alias).setIndices(name).get();
		return response.exists();
	}

	private BulkProcessor getBulkProcessor(Client _client) {
		return BulkProcessor
				.builder(_client, new BulkProcessor.Listener() {
					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
						if (response.hasFailures()) {
							log.error("BulkProcessor error,"
									+ response.buildFailureMessage());
						}
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, Throwable failure) {
						if (failure != null)
							failure.printStackTrace();
					}
				}).setBulkActions(BULK_BUFFER)
				.setBulkSize(new ByteSizeValue(BULK_SIZE, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(BULK_FLUSH))
				.setConcurrentRequests(BULK_CONCURRENT).build();
	}
}
