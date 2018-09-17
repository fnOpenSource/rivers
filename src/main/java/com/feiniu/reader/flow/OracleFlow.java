package com.feiniu.reader.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.model.PipeDataUnit;
import com.feiniu.model.param.TransParam;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.reader.handler.Handler;

public class OracleFlow extends ReaderFlowSocket<HashMap<String, Object>> { 
  
	private final static Logger log = LoggerFactory.getLogger(OracleFlow.class);

	public static OracleFlow getInstance(HashMap<String, Object> connectParams) {
		OracleFlow o = new OracleFlow();
		o.INIT(connectParams);
		return o;
	} 
 

	@Override
	public HashMap<String, Object> getJobPage(HashMap<String, String> param,Map<String, TransParam> transParams,Handler handler) {
		 
		this.jobPage.clear(); 
		PREPARE(false,false); 
		if(!ISLINK())
			return this.jobPage; 
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		boolean releaseConn = false;
		this.jobPage.put(GlobalParam.READER_STATUS,true);
	 
		try (PreparedStatement statement = conn.prepareStatement(param.get("sql"));){  
			statement.setFetchSize(GlobalParam.MAX_PER_PAGE); 
			try(ResultSet rs = statement.executeQuery();){				
				this.jobPage.put(GlobalParam.READER_KEY, param.get(GlobalParam.READER_KEY));
				this.jobPage.put(GlobalParam.READER_SCAN_KEY, param.get(GlobalParam.READER_SCAN_KEY));  
				if(handler==null){
					getAllData(rs,transParams); 
				}else{
					handler.Handle(this,rs,transParams);
				} 
			} catch (Exception e) {
				this.jobPage.put(GlobalParam.READER_STATUS,false);
				log.error("SqlReader init Exception", e);
			} 
		} catch (SQLException e){
			this.jobPage.put(GlobalParam.READER_STATUS,false);
			log.error(param.get("sql") + " getJobPage SQLException", e);
		} catch (Exception e) { 
			releaseConn = true;
			this.jobPage.put(GlobalParam.READER_STATUS,false);
			log.error("getJobPage Exception so free connection,details ", e);
		}finally{
			REALEASE(false,releaseConn);
		} 
		return this.jobPage;
	}

	@Override
	public List<String> getPageSplit(final HashMap<String, String> param) {
		String sql;
		if(param.get("pageSql")!=null){
			sql = " select #{COLUMN} as id,ROWNUM AS FN_ROW_ID from ("
					+ param.get("pageSql")
					+ ") FN_FPG_MAIN  order by #{COLUMN} desc";
		}else{
			sql = " select #{COLUMN} as id,ROWNUM AS FN_ROW_ID from ("
					+ param.get("originalSql")
					+ ") FN_FPG_MAIN  order by #{COLUMN} desc";
		} 
		sql = " select id from (" + sql + ") FN_FPG_END where MOD(FN_ROW_ID, "
				+ GlobalParam.MAX_PER_PAGE + ") = 0";
		sql = sql
				.replace("#{TABLE}", param.get("table"))
				.replace("#{table}", param.get("table"))
				.replace("#{ALIAS}", param.get("alias"))
				.replace("#{alias}", param.get("alias"))
				.replace("#{COLUMN}", param.get("column"))
				.replace("#{column}", param.get("column"))
				.replace(GlobalParam._start_time, param.get(GlobalParam._start_time))
				.replace(GlobalParam._end_time, param.get(GlobalParam._end_time))
				.replace("#{start}", "0")
				.replace("#{START}", "0");
		if (param.get(GlobalParam._seq) != null && param.get(GlobalParam._seq).length() > 0)
			sql = sql.replace(GlobalParam._seq, param.get(GlobalParam._seq));
		 
		List<String> page = new ArrayList<String>();
		PREPARE(false,false); 
		if(!ISLINK())
			return page;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		PreparedStatement statement = null;
		ResultSet rs  = null;
		boolean releaseConn = false;
		try {
			boolean autoSelect = true;
			if(param.get("keyColumnType") != null){
				autoSelect = false;
				if(param.get("keyColumnType").equals("int")){
					statement = conn.prepareStatement(sql.replace("#{end}", Long.MAX_VALUE + "").replace(
							"#{END}", Long.MAX_VALUE + ""));
				}else{
					statement = conn.prepareStatement(sql.replace("#{end}", "~").replace("#{END}", "~")); 
				}
			}else{
				statement = conn.prepareStatement(sql.replace("#{end}", Long.MAX_VALUE + "").replace(
						"#{END}", Long.MAX_VALUE + ""));
			} 
			statement.setFetchSize(GlobalParam.MAX_PER_PAGE);
			rs = statement.executeQuery(); 
			while (rs.next()) { 
				page.add(rs.getString("id"));
			}
			if (autoSelect && page.size() == 0) {
				statement.close();
				rs.close();
				statement = conn.prepareStatement(sql.replace("#{end}", "~").replace("#{END}", "~")); 
				rs = statement.executeQuery();  
				while (rs.next()) {
					page.add(rs.getString("id"));
				}
			}
			Collections.reverse(page);  
		} catch (SQLException e){
			log.error(param.get("sql") + " getPageSplit SQLException", e);
		} catch (Exception e) { 
			releaseConn = true;
			log.error("getPageSplit Exception so free connection,details ", e);
		}finally{ 
			try {
				statement.close();
				rs.close();
			} catch (Exception e) {
				log.error("close connection resource Exception", e);
			} 
			REALEASE(false,releaseConn);
		}  
		return page;
	} 
	 

	private void getAllData(ResultSet rs,Map<String, TransParam> writeParamMap) {  
		this.datas.clear();
		String maxId = null;
		String READER_LAST_STAMP=null;
		try {  
			ResultSetMetaData metaData = rs.getMetaData();
			int columncount = metaData.getColumnCount(); 
			while (rs.next()) {
				PipeDataUnit u = PipeDataUnit.getInstance();
				for (int i = 1; i < columncount + 1; i++) {
					String v = rs.getString(i);
					String k = metaData.getColumnLabel(i);
					if(k.equals(this.jobPage.get(GlobalParam.READER_KEY))){
						u.setKeyColumnVal(v);
						maxId = v;
					}
					if(k.equals(this.jobPage.get(GlobalParam.READER_SCAN_KEY))){
						READER_LAST_STAMP = v;
					}
					u.addFieldValue(k, v, writeParamMap);
				}
				this.datas.add(u);
			}
			rs.close();
		} catch (SQLException e) {
			this.jobPage.put(GlobalParam.READER_STATUS,false);
			log.error("getAllData SQLException,", e);
		}
		if (READER_LAST_STAMP==null){ 
			this.jobPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis()); 
		}else{
			this.jobPage.put(GlobalParam.READER_LAST_STAMP, READER_LAST_STAMP); 
		}
		this.jobPage.put("maxId", maxId);
		this.jobPage.put("datas", this.datas);
	} 
}