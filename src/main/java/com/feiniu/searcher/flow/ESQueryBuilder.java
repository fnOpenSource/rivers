package com.feiniu.searcher.flow;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.QUERY_TYPE;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.ESQueryModel;
import com.feiniu.model.FNRequest;
import com.feiniu.model.param.FNParam;
import com.feiniu.searcher.FNQueryBuilder;
import com.feiniu.util.Common;
import com.feiniu.util.LongRangeType;
 
public class ESQueryBuilder implements FNQueryBuilder{  
	
	private final static Logger log = LoggerFactory.getLogger(ESQueryBuilder.class);
	 
	static public QueryBuilder EmptyQuery()
	{
		return QueryBuilders.termQuery("EMPTY", "0x000");
	}
	
	static public BoolQueryBuilder buildBooleanQuery(FNRequest request, NodeConfig nodeConfig, Analyzer analyzer,
			Map<String, QueryBuilder> attrQueryMap) {
		BoolQueryBuilder bquery = QueryBuilders.boolQuery(); 
		try { 
			Map<String, String> paramMap = request.getParams();  
			Set<Entry<String, String>> entries = paramMap.entrySet();
			Iterator<Entry<String, String>> iter = entries.iterator();
			/**support fuzzy search */
			int fuzzy = 0;
			if(request.getParam(GlobalParam.PARAM_FUZZY)!=null){
				fuzzy = Integer.parseInt(request.getParam(GlobalParam.PARAM_FUZZY)); 
			}
			while (iter.hasNext()) {
				Entry<String, String> entry = iter.next();
				String key = entry.getKey();
				String value = entry.getValue(); 
				Occur occur = Occur.MUST;
				/**support script search */
				if(key.equalsIgnoreCase(GlobalParam.PARAM_DEFINEDSEARCH)){
					if(value.indexOf(GlobalParam.PARAM_ANDSCRIPT)>-1){
						int pos1 = value.indexOf(GlobalParam.PARAM_ANDSCRIPT);
						int pos2 = value.lastIndexOf(GlobalParam.PARAM_ANDSCRIPT);
						if(pos1 != pos2){
							BoolQueryBuilder bbtmp = QueryBuilders.boolQuery();
							bbtmp.must(getScript(value.substring(pos1+GlobalParam.PARAM_ANDSCRIPT.length(),pos2)));
							String qsq = "";
							if(pos1>0)
								qsq+=value.substring(0,pos1);
							if(pos2<value.length()-GlobalParam.PARAM_ANDSCRIPT.length())
								qsq+=value.substring(pos2+GlobalParam.PARAM_ANDSCRIPT.length());
							if(qsq.length()>1)
								bbtmp.must(QueryBuilders.queryStringQuery(qsq));
							bquery.must(bbtmp);
						}
					}else if(value.indexOf(GlobalParam.PARAM_ORSCRIPT)>-1){
						int pos1 = value.indexOf(GlobalParam.PARAM_ORSCRIPT);
						int pos2 = value.lastIndexOf(GlobalParam.PARAM_ORSCRIPT);
						if(pos1 != pos2){
							BoolQueryBuilder bbtmp = QueryBuilders.boolQuery();
							bbtmp.should(getScript(value.substring(pos1+GlobalParam.PARAM_ORSCRIPT.length(),pos2)));
							String qsq = "";
							if(pos1>0)
								qsq+=value.substring(0,pos1);
							if(pos2<value.length()-GlobalParam.PARAM_ORSCRIPT.length())
								qsq+=value.substring(pos2+GlobalParam.PARAM_ORSCRIPT.length());
							if(qsq.length()>1)
								bbtmp.should(QueryBuilders.queryStringQuery(qsq));
							bquery.must(bbtmp);
						}
					}else{
						QueryStringQueryBuilder _q = QueryBuilders.queryStringQuery(value);
						bquery.must(_q);
					} 
					continue;
				}  
				 
				if (key.endsWith(GlobalParam.NOT_SUFFIX)) {
					key = key.substring(0, key.length() - GlobalParam.NOT_SUFFIX.length());
					occur = Occur.MUST_NOT;
				}

				FNParam pr = nodeConfig.getParam(key);
				if (pr == null || Common.isDefaultParam(pr.getName())){ 
					continue;
				}  

				QueryBuilder query = null;
				String multifield = pr.getFields();

				if (multifield != null && multifield.length() > 0)
					query = buildMultiQuery(multifield, value, nodeConfig, request, analyzer, key,fuzzy);
				else
					query = buildSingleQuery(key, value, pr, request, analyzer, key,fuzzy);

				if (occur == Occur.MUST_NOT && query != null) {
					bquery.mustNot(query);
					continue;
				}

			if (query != null)
				bquery.must(query);
			}

		} catch (Exception e) {
			log.error("buildBooleanQuery Exception",e);
		} 
		return bquery;
	} 
	
	@Override
	public ESQueryModel getQuery(FNRequest request) { 
		return null;
	} 
	 
	static private void QueryBoost (QueryBuilder query, FNParam pr, FNRequest request)throws Exception{
		float boostValue = pr.getBoost();

		Method m = query.getClass().getMethod("boost", new Class[]{float.class});
		if (query instanceof FunctionScoreQueryBuilder)
			boostValue = (float) Math.sqrt(boostValue);
		m.invoke(query, boostValue);
	}
	
	static private QueryBuilder buildSingleQuery(String key, String value, FNParam pr, FNRequest request, Analyzer analyzer, String paramKey,int fuzzy) throws Exception{
		if (value == null || value.length() <= 0 || pr == null 
				|| (pr.getDefaultValue() != null && value.equals(pr.getDefaultValue())))
			return null;
		String field = key;

		if (pr.getName() != null)
			field = pr.getName();

		boolean not_analyzed = pr.getAnalyzer().equalsIgnoreCase(GlobalParam.NOT_ANALYZED) ? true : false;
		
		if (!not_analyzed)
			value = value.toLowerCase().trim();
		
		BoolQueryBuilder bquery = QueryBuilders.boolQuery();
		String[] values = value.split(",");
		for (String v : values) {
			QueryBuilder query = null;
			if (!not_analyzed) {
				query = fieldParserQuery(field, String.valueOf(v),fuzzy, analyzer);
			}else if (pr.getIndextype().equalsIgnoreCase("long")){
				Object val = request.get(key, pr);
				if (val instanceof LongRangeType){
					boolean isClosedInvterval = pr.isClosedInvterval(); 
					LongRangeType longRangeValue = LongRangeType.valueOf(v);
					query = QueryBuilders.rangeQuery(field).from(longRangeValue.getMin()).to(longRangeValue.getMax()).includeLower(isClosedInvterval).includeUpper(isClosedInvterval);
				}
				else
					query = QueryBuilders.termQuery(field, String.valueOf(v));
			} 
			else{		
				query = QueryBuilders.termQuery(field,String.valueOf(v));
			} 
			
			if (query != null) {
				QueryBoost(query, pr, request);				
				if (request.getParams().containsKey(field + "_and"))
					bquery.must(query);
				else
					bquery.should(query);
			}
		}

		return bquery;
	} 
	
	static private QueryBuilder fieldParserQuery(String field, String queryStr,int fuzzy, Analyzer analyzer) {
		return fieldParserQuery(field, queryStr, analyzer,fuzzy, ESSimpleQuery.createQuery(QUERY_TYPE.BOOLEAN_QUERY));
	} 
	
	static private QueryBuilder fieldParserQuery(String field, String queryStr, Analyzer analyzer,int fuzzy, ESSimpleQuery ESSimpleQuery) {
		List<String> terms = Common.getKeywords(queryStr, analyzer); 
		for(String term : terms){  
			if(fuzzy>0){  
				FuzzyQueryBuilder fzQuery = QueryBuilders.fuzzyQuery(field, term);
				fzQuery.fuzziness(Fuzziness.TWO);
				fzQuery.maxExpansions(fuzzy); 
				ESSimpleQuery.add(new BoolQueryBuilder().should(fzQuery).should(QueryBuilders.termQuery(field, term).boost(1.2f)),"must");   
			}else{ 
				ESSimpleQuery.add(QueryBuilders.termQuery(field, term),"must");
			}  
		}
		return ESSimpleQuery.getQuery();
	} 

	static private QueryBuilder buildMultiQuery(String multifield, String value, NodeConfig prs, FNRequest request, Analyzer analyzer, String paramKey,int fuzzy) throws Exception {
		DisMaxQueryBuilder bquery = null; 
		String[] keys = multifield.split(",");

		if (keys.length <= 0)
			return null;

		if (keys.length == 1) {
			FNParam pr = prs.getParam(keys[0]);
			return buildSingleQuery(keys[0], value, pr, request, analyzer, paramKey,fuzzy);
		}

		String[] word_vals = value.split(",");
		for (String word : word_vals) {
			BoolQueryBuilder subquery2 = null;
			List<String> vals = Common.getKeywords(word, analyzer);
  
			for (String val : vals) {
				DisMaxQueryBuilder parsedDisMaxQuery = null;
				for (String key2 : keys) {
					FNParam pr2 = prs.getParam(key2);
					QueryBuilder query = buildSingleQuery(key2, pr2.getAnalyzer().equals("NOT_ANALYZED")? word : val, pr2, request, analyzer, paramKey,fuzzy);
					if (query != null) {
						if (parsedDisMaxQuery == null)
							parsedDisMaxQuery = QueryBuilders.disMaxQuery().tieBreaker(GlobalParam.DISJUNCTION_QUERY_WEIGHT);
						parsedDisMaxQuery.add(query);
					}
				}
				if (parsedDisMaxQuery != null) {
					if (subquery2 == null)
						subquery2 = QueryBuilders.boolQuery();
					subquery2.must(parsedDisMaxQuery);
				}
			}

			if (subquery2 != null) {
				if (bquery == null)
					bquery = QueryBuilders.disMaxQuery().tieBreaker(GlobalParam.DISJUNCTION_QUERY_WEIGHT);
				bquery.add(subquery2);
			}
		}
		return bquery;
	}
	 
	static private QueryBuilder getScript(String str){
		return QueryBuilders.scriptQuery(
			    new Script(
			    		str.replace("\\", ""),                    
			        ScriptType.INLINE,       
			        "groovy", null)        
			); 
	} 
}


class ESSimpleQuery{
	private QueryBuilder innerQuery = null;
	
	public static ESSimpleQuery createQuery(QUERY_TYPE query_type){
		QueryBuilder query = null;
		if (query_type == QUERY_TYPE.BOOLEAN_QUERY)
			query = QueryBuilders.boolQuery();
		else if (query_type == QUERY_TYPE.DISJUNCTION_QUERY)
			query = QueryBuilders.disMaxQuery().boost(GlobalParam.DISJUNCTION_QUERY_WEIGHT); 
		return new ESSimpleQuery(query);
	}
	
	private ESSimpleQuery(QueryBuilder query)
	{
		this.innerQuery = query;
	}
	
	public void add(QueryBuilder query,String type){
		if (innerQuery instanceof DisMaxQueryBuilder){
			((DisMaxQueryBuilder)innerQuery).add(query);
		}else{
			if(type.equals("must"))
				((BoolQueryBuilder)innerQuery).must(query);
			else
				((BoolQueryBuilder)innerQuery).should(query);
		} 
	}
	
	public QueryBuilder getQuery()
	{
		return innerQuery;
	}
}
