package com.feiniu.searcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.config.GlobalParam.KEY_PARAM;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.ESQueryModel;
import com.feiniu.model.FNQuery;
import com.feiniu.model.FNRequest;
import com.feiniu.model.FNResponse;
import com.feiniu.model.SolrQueryModel;
import com.feiniu.model.param.FNParam;
import com.feiniu.searcher.flow.ESQueryBuilder;
import com.feiniu.searcher.flow.SearcherFlowSocket;
import com.feiniu.searcher.flow.SolrQueryBuilder;

/**
 * provide search service
 * 
 * @author chengwen
 * @version 1.0
 */
public class FNSearcher {
	private final static Logger log = LoggerFactory.getLogger(FNSearcher.class);
	private SearcherFlowSocket searcher = null;
	private NodeConfig NodeConfig = null;
	private String instanceName = null;

	public static FNSearcher getInstance(String instanceName,
			NodeConfig NodeConfig, SearcherFlowSocket searcher) {
		return new FNSearcher(instanceName, NodeConfig, searcher);
	}

	private FNSearcher(String instanceName, NodeConfig NodeConfig,
			SearcherFlowSocket searcher) {
		this.instanceName = instanceName;
		this.searcher = searcher;
		this.NodeConfig = NodeConfig;
	}

	public FNResponse startSearch(FNRequest rq) {
		FNResponse response = FNResponse.getInstance();
		response.setIndex(instanceName);
		/** check validation */
		if (!rq.isValid()) {
			response.setError_info("handle is null!");
			return response;
		}

		if (this.searcher == null) {
			response.setError_info("searcher is null!");
			response.setParams(rq.getParams(), null);
			return response;
		}
		response.setParams(rq.getParams(), NodeConfig);
		Analyzer analyzer = this.searcher.getAnalyzer();

		FNQuery<?, ?, ?> query;
		if (this.searcher.getType() == DATA_TYPE.ES) {
			query = getEsQuery(rq, analyzer);
		} else {
			query = getSolrQuery(rq, analyzer);
		}
		try {
			response.setError_info(rq.getErrors());
			response.setResult(this.searcher.Search(query, instanceName));
		} catch (Exception e) {
			log.error("FNResponse error,", e);
		}
		return response;
	}

	private void initQuery(FNRequest request, FNQuery<?, ?, ?> fq) {
		Object o = request.get(GlobalParam.KEY_PARAM.start.toString(),
				NodeConfig.getParam(KEY_PARAM.start.toString()));
		if (o != null) {
			int start = (int) o;
			if (start >= 0)
				fq.setStart(start);
		}
		o = request.get(KEY_PARAM.count.toString(),
				NodeConfig.getParam(KEY_PARAM.count.toString()));
		if (o != null) {
			int count = (int) o;
			if (count >= 1 && count <= 2000) {
				fq.setCount(count);
			}
		}
		if (request.getParams().containsKey(GlobalParam.PARAM_SHOWQUERY))
			fq.setShowQueryInfo(true);
		if (request.getParams().containsKey(GlobalParam.PARAM_FL))
			fq.setFl(request.getParam(GlobalParam.PARAM_FL));
		if (request.getParams().containsKey(GlobalParam.PARAM_FQ))
			fq.setFq(request.getParam(GlobalParam.PARAM_FQ));
	}

	private SolrQueryModel getSolrQuery(FNRequest request, Analyzer analyzer) {
		SolrQueryModel sq = new SolrQueryModel();
		initQuery(request, sq);
		sq.setQuery(SolrQueryBuilder.queryBuilder(request, NodeConfig,
				analyzer, new HashMap<String, String>()));
		return sq;
	}

	private ESQueryModel getEsQuery(FNRequest request, Analyzer analyzer) {
		ESQueryModel eq = new ESQueryModel();
		initQuery(request, eq);
		eq.setSorts(getSortField(request, NodeConfig));
		eq.setFacetSearchParams(getFacetParams(request, NodeConfig));
		Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>();
		BoolQueryBuilder query = ESQueryBuilder.buildBooleanQuery(request,
				NodeConfig, analyzer, attrQueryMap);
		eq.setQuery(query);
		eq.setAttrQueryMap(attrQueryMap);
		return eq;
	}

	private List<SortBuilder> getSortField(FNRequest request, NodeConfig config) { 
		FNParam pr = config.getParam(KEY_PARAM.sort.toString());
		String sortby = (String) request.get(KEY_PARAM.sort.toString(), pr);
		List<SortBuilder> sortList = new ArrayList<SortBuilder>();
		boolean useScore = false;
		if (sortby != null && sortby.length() > 0) { 
			boolean reverse = false;
			String[] sortstrs = sortby.split(",");
			String fieldname = ""; 
			for (String sortstr : sortstrs) {
				sortstr = sortstr.trim();
				if (sortstr.endsWith(GlobalParam.SORT_DESC)) {
					reverse = true;
					fieldname = sortstr.substring(0,
							sortstr.indexOf(GlobalParam.SORT_DESC));
				} else if (sortstr.endsWith(GlobalParam.SORT_ASC)) {
					reverse = false;
					fieldname = sortstr.substring(0,
							sortstr.indexOf(GlobalParam.SORT_ASC));
				} else {
					reverse = false;
					fieldname = sortstr;
				}

				switch (fieldname) {
				case GlobalParam.PARAM_FIELD_SCORE:
					sortList.add(SortBuilders.scoreSort().order(
							reverse ? SortOrder.DESC : SortOrder.ASC));
					useScore = true;
					break;
				case GlobalParam.PARAM_FIELD_RANDOM:
					sortList.add(SortBuilders.scriptSort(
							new Script("random()"), "number"));
					break;

				default:
					FNParam sortpr = config.getParam(fieldname);
					if (sortpr == null)
						continue;
					String realFieldName = sortpr.getName();
					if (realFieldName == null || realFieldName.length() <= 0)
						continue;
					sortList.add(SortBuilders.fieldSort(realFieldName).order(
							reverse ? SortOrder.DESC : SortOrder.ASC));
					break;
				}
			}
		} 
		if (!useScore)
			sortList.add(SortBuilders.scoreSort().order(SortOrder.DESC));
		return sortList;
	}

	private Map<String, List<String[]>> getFacetParams(FNRequest request,
			NodeConfig prs) {
		Map<String, List<String[]>> ret = new HashMap<String, List<String[]>>();

		return ret;
	}

}
