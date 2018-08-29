package com.feiniu.config;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;

import com.feiniu.model.RiverState;
import com.feiniu.node.NodeMonitor;
import com.feiniu.node.SocketCenter;
import com.feiniu.task.FlowTask;
import com.feiniu.task.TaskManager;
import com.feiniu.util.Common;
import com.feiniu.util.email.FNEmailSender;
/**
 * global node data store position  
 * @author chengwen
 * @version 1.0 
 */
public class GlobalParam {
	
	public static String run_environment;
	
	public static String VERSION;
	/**node core writer status; 0 stop, 1 ready, 2 running, 4 Termination */
	public volatile static RiverState<AtomicInteger> FLOW_STATUS = new RiverState<AtomicInteger>();
	/**FLOW_INFOS current flow runing state information*/
	public volatile static RiverState<HashMap<String,String>> FLOW_INFOS = new RiverState<HashMap<String,String>>();
	
	public volatile static RiverState<String> LAST_UPDATE_TIME = new RiverState<String>();
	
	public static boolean WRITE_BATCH = false;
	/**#1 searcher service  2 writer service 4 http reader service 8 instruction service*/
	public static int SERVICE_LEVEL;
	
	public static Analyzer SEARCH_ANALYZER; 
	
	public static String CONFIG_PATH;
	
	public static String IP;
	
	public static SocketCenter SOCKET_CENTER;
	
	public static TaskManager TASKMANAGER;
	
	public static NodeMonitor nodeMonitor; 
	
	public static FNEmailSender mailSender; 
	
	public static int POOL_SIZE = 6;
	/** CONNECT_EXPIRED is milliseconds time */
	public static int CONNECT_EXPIRED = 7200000; 
	
	public static NodeConfig nodeConfig;
	
	public static HashMap<String, FlowTask> tasks; 
	
	 
	 
	//writer parameters
	public static enum KEY_PARAM {
		start, count, sort, facet, detail, facet_count,group,fl
	}  
	public static enum DATA_TYPE{
		MYSQL, ORACLE, HIVE, ES, SOLR, HBASE,ZOOKEEPER,UNKNOWN,H2
	} 
	public final static String DEFAULT_RESOURCE_TAG = "_DEFAULT"; 
	public final static String DEFAULT_FIELD = "SYSTEM_UPDATE_TIME"; 
	public final static String DEFAULT_RESOURCE_SEQ = ""; 
	public static enum JOB_TYPE {
		FULL,INCREMENT
	} 
	public static enum FLOWINFO{
		MASTER,FULL_STATE,FULL_STOREID,INCRE_STOREID,FULL_JOBS
	} 
	public final static String JOB_STATE_SPERATOR = ":"; 
	public final static String JOB_INCREMENTINFO_PATH = "batch";  
	public final static String JOB_FULLINFO_PATH = "full_info"; 
	
	//searcher parameters
	public static int SEARCH_MAX_WINDOW=20000; 
	public static int SEARCH_MAX_PAGE=2000;
	public final static String NOT_ANALYZED = "NOT_ANALYZED"; 
	public final static float DISJUNCTION_QUERY_WEIGHT = 0.1f; 
	public final static int FACET_DEFAULT_COUNT = 200;
	public final static int FACET_DEAULT_SHOW_COUNT = 3;  
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
	public final static String NOT_SUFFIX = "_not";
	public final static String PARAM_REQUEST_HANDLER = "request_handler";
	
	public static enum QUERY_TYPE {  
		BOOLEAN_QUERY, DISJUNCTION_QUERY 
	}    

	//reader parameters
	public static final String _start = "#{start}";
	public static final String _end = "#{end}"; 
	public static final String _seq = "#{seq}"; 
	public static final int MAX_PER_PAGE = 10000;
	public static final String _table = "#{table}";
	public static final String _column = "#{column}";
	public static final String _incrementField = "#{update_time}"; 
	public static final String _start_time =  "#{start_time}"; 
	public static final String _end_time =  "#{end_time}";  
	public static final String READER_KEY = "keyColumn";
	public static final String READER_SCAN_KEY = "IncrementColumn";
	public static final String READER_LAST_STAMP = "lastUpdateTime";
	public static final String READER_STATUS = "_reader_status"; 
	
	
	static {
		try {
			IP = InetAddress.getLocalHost().getHostAddress();
		}catch (Exception e) {
			Common.LOG.error("getHostAddress Exception ",e);
		} 
	}
}
