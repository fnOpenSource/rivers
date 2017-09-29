package com.feiniu.config;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;

import com.feiniu.node.NodeCenter;
import com.feiniu.task.Task;
import com.feiniu.util.email.FNEmailSender;
/**
 * global node data store position  
 * @author chengwen
 * @version 1.0 
 */
public class GlobalParam {
	
	public static String run_environment;
	
	public static String VERSION;
	/**node core writer status,0 stop 1 normal 2running 4break running thread */
	public static HashMap<String,AtomicInteger> FLOW_STATUS = new HashMap<String, AtomicInteger>();
	/**FLOW_INFOS current flow runing state information*/
	public static HashMap<String,HashMap<String,String>> FLOW_INFOS = new HashMap<String, HashMap<String,String>>();
	
	public static boolean WRITE_BATCH = false;
	
	public static Analyzer SEARCH_ANALYZER; 
	
	public static String CONFIG_PATH;
	
	public static NodeCenter NODE_CENTER;
	
	public static ConcurrentHashMap<String,String> LAST_UPDATE_TIME = new ConcurrentHashMap<String, String>();
	
	public static FNEmailSender mailSender;
	
	public static int port = 9800;
	
	public static int POOL_SIZE = 6;
	
	public static NodeTreeConfigs nodeTreeConfigs;
	
	public static HashMap<String, Task> tasks; 
	
	public static enum KEY_PARAM {
		start, count, sort, facet, detail, facet_count,group,fl
	} 
	
	public static enum DATA_TYPE{
		MYSQL, ORACLE, HIVE, ES, SOLR, HBASE,ZOOKEEPER,UNKNOWN
	} 
	 
	public static enum QUERY_TYPE {  
		BOOLEAN_QUERY, DISJUNCTION_QUERY 
	}   
	
	
	public final static String NOT_ANALYZED = "NOT_ANALYZED"; 
	public final static float DISJUNCTION_QUERY_WEIGHT = 0.1f;
		
	public final static char FACET_DELIMITER = '/';
	public final static String FACET_DELIMITER_STR = "/";
	public final static int FACET_DEFAULT_COUNT = 200;
	public final static int FACET_DEAULT_SHOW_COUNT = 3;
	public final static int FACET_UNLIMITED_SHOW_COUNT = 99; 
	
	public final static String FACET_SUFFIX = "_attr";

	public final static String SORT_ASC = "_asc";
	public final static String SORT_DESC = "_desc";
 
	public final static String PARAM_FL = "fl";
	public final static String PARAM_FQ = "fq";
	public final static String PARAM_FUZZY = "fuzzy";
	public final static String PARAM_KEYWORD = "keyword"; 
	public final static String PARAM_GROUP = "group"; 
	public final static String PARAM_ANDSCRIPT = "ANDscript";
	public final static String PARAM_ORSCRIPT = "ORscript";
	public final static String PARAM_SCRIPT_TYPE = "script_type";
	public final static String PARAM_DEFINEDSEARCH = "search_dsl";
	public final static String PARAM_SHOWQUERY = "showquery";
	public final static String PARAM_FIELD_SCORE = "field_score";
	public final static String PARAM_FIELD_RANDOM = "field_random";
	public final static String PARAM_FACET_ORIGINAL = "facet_original";
	public final static String PARAM_FACET = "facet";
	public final static String PARAM_FACET_EXT = "facet_ext";
	public final static String PARAM_SORT = "sort";
	public final static String PARAM_INVALID = "invalid"; 
	public final static String NOT_SUFFIX = "_not";
	 
	public static final String _start = "#{start}";
	public static final String _end = "#{end}"; 
	public static final String _seq = "#{seq}"; 
	public static final int MAX_PER_PAGE = 10000;
	public static final String _table = "#{table}";
	public static final String _column = "#{column}";
	public static final String _incrementField = "#{update_time}"; 
	public static final String _start_time =  "#{start_time}"; 
}
