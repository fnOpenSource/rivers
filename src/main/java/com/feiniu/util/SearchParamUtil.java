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
import com.feiniu.model.FNQuery;
import com.feiniu.model.FNRequest;
import com.feiniu.model.param.FNParam;

public class SearchParamUtil {

	public static void reWriteParam(FNRequest request, FNQuery<?, ?, ?> fq,NodeConfig nodeConfig) {
		Object o = request.get(GlobalParam.KEY_PARAM.start.toString(),
				nodeConfig.getParam(KEY_PARAM.start.toString()));
		if (o != null) {
			int start = (int) o;
			if (start >= 0)
				fq.setStart(start);
		}
		o = request.get(KEY_PARAM.count.toString(),
				nodeConfig.getParam(KEY_PARAM.count.toString()));
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
	
	public static List<SortBuilder> getSortField(FNRequest request, NodeConfig config) { 
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
					if (fieldname == null || fieldname.length() <= 0)
						continue;
					sortList.add(SortBuilders.fieldSort(fieldname).order(
							reverse ? SortOrder.DESC : SortOrder.ASC));
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
		public static Map<String,List<String[]>> getFacetParams(FNRequest rq,
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