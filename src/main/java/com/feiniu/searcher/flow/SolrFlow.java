package com.feiniu.searcher.flow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.NodeConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.handler.ConnectionHandler;
import com.feiniu.model.FNDataUnit;
import com.feiniu.model.FNQuery;
import com.feiniu.model.FNResultSet;
import com.feiniu.model.param.WriteParam;
import com.feiniu.util.FNException;

public class SolrFlow extends SearcherFlowSocket { 
	
	private String collectionName = "";
	private long storetime = 0; 
	private ConnectionHandler handler;

	private final static Logger log = LoggerFactory.getLogger(SolrFlow.class);

	public static SolrFlow getInstance(HashMap<String, Object> connectParams) {
		SolrFlow o = new SolrFlow();
		o.INIT(connectParams);
		return o;
	}
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.NodeConfig = (NodeConfig) this.connectParams.get("nodeConfig");
		this.analyzer = (Analyzer) this.connectParams.get("analyzer");
		if(this.connectParams.get("handler")!=null){ 
			try {
				this.handler = (ConnectionHandler)Class.forName((String) this.connectParams.get("handler")).newInstance();
				this.handler.init(connectParams);
			} catch (Exception e) {
				log.error("Init handler Exception",e);
			}
		} 
	} 

	@Override
	public FNResultSet Search(FNQuery<?, ?, ?> fq, String index) throws FNException{
		FnConnection<?> FC = PULL(true);
		FNResultSet ret = new FNResultSet();
		try {
			CloudSolrClient conn = (CloudSolrClient) FC.getConnection();
			int start = fq.getStart();
			int count = fq.getCount();
			SolrQuery qb = (SolrQuery) fq.getQuery();
			qb.setParam("defType", "edismax");
			QueryResponse response = getSearchResponse(conn, qb, index, start,
					count, fq);
			NamedList<Object> commonResponse = response.getResponse();
			if (fq.isShowQueryInfo()) {
				ret.setQueryDetail(qb.toString()); 
				ret.setExplainInfo(response.getExplainMap());
			}
			for (int i = 0; i < commonResponse.size(); i++) {
				String name = commonResponse.getName(i);
				Object value = commonResponse.getVal(i);
				if (name.equals("response")) {
					SolrDocumentList v = (SolrDocumentList) value;
					ret.setTotalHit((int) v.getNumFound());
				}
			}
			addResult(ret, response,fq);
			if (response.getFacetFields() != null) {
				Map<String, Map<String, Integer>> fc = new HashMap<String, Map<String, Integer>>();
				List<FacetField> fields = response.getFacetFields();
				for (FacetField facet : fields) {
					Map<String, Integer> _row = new HashMap<String, Integer>();
					List<Count> counts = facet.getValues();
					for (Count c : counts) {
						_row.put(c.getName(), (int) c.getCount());
					}
					fc.put(facet.getName(), _row);
				}
				ret.setFacetMap(fc);
			}
		}catch(Exception e){ 
			throw new FNException("Search data from Solr exception!");
		}finally{
			CLOSED(FC);
		} 
		return ret;
	} 
	
	private String getCollection() {
		if (this.handler!=null) { 
			if(this.collectionName.length() < 1
					|| System.currentTimeMillis() / 1000 - this.storetime > 120){ 
				this.collectionName = this.handler.getData();
				this.storetime = System.currentTimeMillis() / 1000;
			}  
		}else{
			this.collectionName = String.valueOf(this.connectParams.get("defaultValue"));
		}
		return this.collectionName;
	} 
	
	private void addResult(FNResultSet rs, QueryResponse qs,FNQuery<?, ?, ?> fq) { 
		GroupResponse groupResponse = qs.getGroupResponse();
		NamedList<Object> commonResponse = qs.getResponse();
		boolean setnum = true;
		if (groupResponse != null) {
			List<GroupCommand> groupList = groupResponse.getValues();
			for (GroupCommand groupCommand : groupList) {
				if (setnum) {
					rs.setTotalHit(groupCommand.getNGroups());
				}
				setnum = false;
				List<Group> tmps = groupCommand.getValues();
				for (Group g : tmps) {
					FNDataUnit u = FNDataUnit.getInstance();
					u.addObject(g.getGroupValue(), g.getResult());
					rs.getUnitSet().add(u);
				}
			}
		} else {
			for (int i = 0; i < commonResponse.size(); i++) {
				String name = commonResponse.getName(i);
				Object value = commonResponse.getVal(i);
				if (name.equals("response")) {
					SolrDocumentList v = (SolrDocumentList) value;
					if (setnum)
						rs.setTotalHit((int) v.getNumFound());
					setnum = false;
					for (SolrDocument sd : v) {
						FNDataUnit u = FNDataUnit.getInstance();
						for (String n : sd.getFieldNames()) {
							if(fq.isShowQueryInfo()){ 
								if(n.equals("score")){
									u.addObject("_SCORE", sd.get(n));
									continue;
								} 
							}
							u.addObject(n, sd.get(n));
						}
						rs.getUnitSet().add(u);
					}
					break;
				}
			}
		}
	}

	private QueryResponse getSearchResponse(CloudSolrClient conn,SolrQuery qb, String index,
			int start, int count, FNQuery<?, ?, ?> fq) {
		conn.setDefaultCollection(getCollection());
		qb.setParam("wt", "json");
		qb.setRows(count);
		qb.setStart(start);
		
		if(fq.getFq().length() > 0){
			qb.setParam("fq", fq.getFq());
		}

		String fl = "";
		if (fq.getFl().length() > 0) {
			fl = fq.getFl(); 
		} else { 
			for (Map.Entry<String, WriteParam> e : NodeConfig
					.getWriteParamMap().entrySet()) {
				if (e.getValue().getStored().equalsIgnoreCase("true"))
					fl += "," + e.getKey();
			} 
			if (fl.length() > 1) {
				fl = fl.substring(1);
			}
		}
		if (fq.isShowQueryInfo()){
			qb.set("debug", true);
			qb.add("fl", fl+",score"); 
		}else{
			qb.add("fl", fl); 
		} 
		try {
			return conn.query(qb);
		} catch (Exception e) {
			log.error("getSearchResponse Exception,", e);
		}
		return null;
	} 
}
