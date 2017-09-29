package com.feiniu.searcher.flow;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.client.solrj.SolrQuery;

import com.feiniu.config.NodeConfig;
import com.feiniu.config.GlobalParam;
import com.feiniu.model.FNRequest;
import com.feiniu.model.param.FNParam;
import com.feiniu.util.Common;

public class SolrQueryBuilder {

	public static SolrQuery queryBuilder(FNRequest request, NodeConfig prs,
			Analyzer analyzer, Map<String, String> attrQueryMap) {
		SolrQuery sq = new SolrQuery();
		StringBuffer qr = new StringBuffer();
		Map<String, String> paramMap = request.getParams();
		Set<Entry<String, String>> entries = paramMap.entrySet();
		Iterator<Entry<String, String>> iter = entries.iterator();
		boolean start = true;
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			String k = entry.getKey();
			String v = entry.getValue();

			if (Common.isDefaultParam(k)) {
				switch (k) {
				case "sort":
					sq.setParam("sort", getSortInfo(v, sq));
					break;
				case "group":
					sq.setParam("group", getGroupInfo(v, sq));
					sq.setParam("group.ngroups", "true");
					break; 
				case "facet":
					sq.setParam("facet", getFacetInfo(v, sq));
					break;
				}
				continue;
			}
			if(k.equals(GlobalParam.PARAM_DEFINEDSEARCH)){
				qr.append("("+v+")");
				start=false;
				continue;
			}
			
			FNParam pr = prs.getParam(k);
			if (pr == null){
				continue; 
			} 
			if (!start) {
				qr.append(" AND ");
			} 
			String combineSearch = pr.getFields();
			if(combineSearch != null && combineSearch.length() > 0){
				qr.append(buildMultiQuery(v, combineSearch, paramMap));
			}else{
				qr.append(buildSingleQuery(k, v));
			}
			
			start = false;
		}
		if (qr.length() < 1) {
			sq.setQuery("*:*");
		} else {
			sq.setQuery("_query_:" + qr);
		}
		return sq;
	} 
	static String buildMultiQuery(String v,String combineSearch,Map<String, String> paramMap){ 
		StringBuffer sb = new StringBuffer();
		for(String k:combineSearch.split(",")){
			sb.append(buildSingleQuery(k,v));
		}
		return sb.toString();
	}
	static String buildSingleQuery(String k,String v){
		if (k.endsWith(GlobalParam.NOT_SUFFIX)) {
			k = k.substring(0, k.length() - GlobalParam.NOT_SUFFIX.length());
			return "( NOT " + k + ":" + v + ")";
		} else { 
			return "(" + k + ":" + v + ")";
		}
	}
	static String getGroupInfo(String strs, SolrQuery sq) {
		for (String s : strs.split(",")) {
			String[] tmp = s.split(":");
			if (tmp.length != 2) {
				continue;
			}
			if (tmp[0].equals("group.field")) {
				sq.setParam("group.field", tmp[1]);
			}
			if (tmp[0].equals("group.query")) {
				sq.setParam("group.query", tmp[1]);
			}
			if (tmp[0].equals("group.ngroups")) {
				sq.setParam("group.ngroups", tmp[1]);
			}
			if (tmp[0].equals("group.limit")) {
				sq.setParam("group.limit", tmp[1]);
			}
			if (tmp[0].equals("group.sort")) {
				sq.setParam("group.sort", tmp[1]);
			}
		}
		return "true";
	}

	static String getFacetInfo(String strs, SolrQuery sq) {
		for (String s : strs.split(",")) {
			String[] tmp = s.split(":");
			if (tmp.length != 2) {
				continue;
			}
			if (tmp[0].equals("facet.query")) {
				sq.setParam("facet.query", tmp[1]);
			}
			if (tmp[0].equals("facet.field")) {
				sq.setParam("facet.field", tmp[1]);
			}
			if (tmp[0].equals("facet.limit")) {
				sq.setParam("facet.limit", tmp[1]);
			}
			if (tmp[0].equals("facet.limit")) {
				sq.setParam("facet.limit", tmp[1]);
			}
			if (tmp[0].equals("facet.sort")) {
				sq.setParam("facet.sort", tmp[1]);
			} 
			if (tmp[0].equals("facet.mincount")) {
				sq.setParam("facet.mincount", tmp[1]);
			} 
		}
		return "true";
	}

	static String getSortInfo(String strs, SolrQuery sq) {
		StringBuffer sf = new StringBuffer();
		sf.append("score desc");
		for (String str : strs.split(",")) {
			str = str.trim();
			if (str.endsWith(GlobalParam.SORT_DESC)) {
				sf.append(",");
				sf.append(str.substring(0, str.indexOf(GlobalParam.SORT_DESC)));
				sf.append(" desc"); 
			} else if (str.endsWith(GlobalParam.SORT_ASC)) {
				sf.append(",");
				sf.append(str.substring(0, str.indexOf(GlobalParam.SORT_ASC)));
				sf.append(" asc"); 
			}
		}
		return sf.toString();
	}

}
