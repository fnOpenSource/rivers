package com.feiniu.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.KEY_PARAM;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.SearcherRequest;
import com.feiniu.model.param.SearchParam;
import com.feiniu.model.param.TransParam; 

public class SearchParamUtil {

	public static void normalParam(SearcherRequest request, SearcherModel<?, ?, ?> fq,NodeConfig nodeConfig) {
		Object o = request.get(GlobalParam.KEY_PARAM.start.toString(),
				nodeConfig.getSearchParam(KEY_PARAM.start.toString()),"java.lang.Integer");
		if (o != null) {
			int start = (int) o;
			if (start >= 0)
				fq.setStart(start);
		}
		o = request.get(KEY_PARAM.count.toString(),
				nodeConfig.getSearchParam(KEY_PARAM.count.toString()),"java.lang.Integer");
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
		if (request.getParams().containsKey(GlobalParam.PARAM_REQUEST_HANDLER))
			fq.setRequestHandler(request.getParam(GlobalParam.PARAM_REQUEST_HANDLER));
	}
	
	public static List<SortBuilder> getSortField(SearcherRequest request, NodeConfig nodeConfig) {  
		String sortstrs = request.getParam(KEY_PARAM.sort.toString());
		List<SortBuilder> sortList = new ArrayList<SortBuilder>();
		boolean useScore = false;
		if (sortstrs != null && sortstrs.length() > 0) { 
			boolean reverse = false;
			String[] sortArr = sortstrs.split(",");
			String fieldname = ""; 
			for (String str : sortArr) {
				str = str.trim();
				if (str.endsWith(GlobalParam.SORT_DESC)) {
					reverse = true;
					fieldname = str.substring(0,
							str.indexOf(GlobalParam.SORT_DESC));
				} else if (str.endsWith(GlobalParam.SORT_ASC)) {
					reverse = false;
					fieldname = str.substring(0,
							str.indexOf(GlobalParam.SORT_ASC));
				} else {
					reverse = false;
					fieldname = str;
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
					TransParam checked; 
					SearchParam sp;
					if ((checked = nodeConfig.getTransParam(fieldname)) != null) { 
						sortList.add(SortBuilders.fieldSort(checked.getAlias()).order(
								reverse ? SortOrder.DESC : SortOrder.ASC));
					}else if((sp = nodeConfig.getSearchParam(fieldname))!=null){
						String fields = sp.getFields();
						if(fields!=null) {
							for(String k:fields.split(",")) {
								sortList.add(SortBuilders.fieldSort(k).order(
										reverse ? SortOrder.DESC : SortOrder.ASC));
							}
						}
					}else if(fieldname.equals(GlobalParam.DEFAULT_FIELD)) { 
						sortList.add(SortBuilders.fieldSort(fieldname).order(
								reverse ? SortOrder.DESC : SortOrder.ASC));
					} 
					break;
				}
			}
		} 
		if (!useScore)
			sortList.add(SortBuilders.scoreSort().order(SortOrder.DESC));
		return sortList;
	}
	
	/**
	 * main:funciton:field,son:function:field#new_main:funciton:field
	 * @param rq
	 * @param prs
	 * @return
	 */
		public static Map<String,List<String[]>> getFacetParams(SearcherRequest rq,
				NodeConfig prs) {
			Map<String,List<String[]>> res = new LinkedHashMap<String,List<String[]>>();
			if(rq.getParam("facet")!=null){
				for(String pair:rq.getParams().get("facet").split("#")){
					String[] tmp = pair.split(",");
					List<String[]> son = new ArrayList<>();
					for(String str:tmp) {
						String[] tp = str.split(":");
						if(tp.length>3) {
							String[] tp2= {"","",""};
							for(int i=0;i<tp.length;i++) {
								if(i<2) {
									tp2[i] = tp[i];
								}else {
									if(tp2[2].length()>0) {
										tp2[2] = tp2[2]+":"+tp[i];
									}else {
										tp2[2] = tp[i];
									}
									
								} 
							} 
							son.add(tp2);
						}else {
							son.add(tp);
						}
						
					}
					res.put(tmp[0].split(":")[0], son);
				}
			} 
			return res;
		}

	
}
