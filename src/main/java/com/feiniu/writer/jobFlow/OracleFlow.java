package com.feiniu.writer.jobFlow;

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
import com.feiniu.connect.FnConnection;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;
import com.feiniu.reader.handler.Handler;

public class OracleFlow extends WriteFlowSocket<HashMap<String, Object>> { 
  
	private final static Logger log = LoggerFactory.getLogger(OracleFlow.class);

	public static OracleFlow getInstance(HashMap<String, Object> connectParams) {
		OracleFlow o = new OracleFlow();
		o.INIT(connectParams);
		return o;
	} 

	@Override
	public HashMap<String, Object> getJobPage(HashMap<String, String> param,Map<String, WriteParam> writeParamMap,Handler handler) {
		try {
			while (isLocked.get()) {
				Thread.sleep(1000);
			}
		} catch (Exception e) {
		} 
		isLocked.set(true);
		FnConnection<?> FC = PULL(false);
		this.jobPage.clear(); 
		try {
			Connection conn = (Connection) FC.getConnection();
			PreparedStatement statement = conn.prepareStatement(param.get("sql"));
			statement.setFetchSize(GlobalParam.MAX_PER_PAGE);
			ResultSet rs = statement.executeQuery();
			try {				
				this.jobPage.put("keyColumn", param.get("keyColumn"));
				this.jobPage.put("IncrementColumn", param.get("incrementField"));  
				if(handler==null){
					getAllData(rs,writeParamMap); 
				}else{
					handler.Handle(this,rs,writeParamMap);
				} 
			} catch (Exception e) {
				this.jobPage.put("lastUpdateTime", -1);
				log.error("SqlReader init Exception", e);
			}
			statement.close();
			rs.close();
		} catch (Exception e) {
			log.error(param.get("sql") + " getJobPage Exception", e);
		} 
		CLOSED(FC);
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
				.replace(GlobalParam._start_time, param.get("startTime"))
				.replace("#{start}", "0")
				.replace("#{START}", "0");
		if (param.get("seq") != null && param.get("seq").length() > 0)
			sql = sql.replace(GlobalParam._seq, param.get("seq"));
		FnConnection<?> FC = PULL(false);;
		Connection conn = (Connection) FC.getConnection();
		List<String> page = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet rs  = null;
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
		} catch (Exception e) {
			log.error("getPageSplit Exception", e);
		}finally{ 
			try {
				statement.close();
				rs.close();
			} catch (Exception e) {
				log.error("close connection resource Exception", e);
			} 
			CLOSED(FC);
		}  
		return page;
	} 
	 

	private void getAllData(ResultSet rs,Map<String, WriteParam> writeParamMap) {  
		this.datas.clear();
		String maxId = null;
		String updateFieldValue=null;
		try {  
			ResultSetMetaData metaData = rs.getMetaData();
			int columncount = metaData.getColumnCount(); 
			while (rs.next()) {
				WriteUnit u = WriteUnit.getInstance();
				for (int i = 1; i < columncount + 1; i++) {
					String v = rs.getString(i);
					String k = metaData.getColumnLabel(i);
					if(k.equals(this.jobPage.get("keyColumn"))){
						u.setKeyColumnVal(v);
						maxId = v;
					}
					if(k.equals(this.jobPage.get("IncrementColumn"))){
						updateFieldValue = v;
					}
					u.addFieldValue(k, v, writeParamMap);
				}
				this.datas.add(u);
			}
			rs.close();
		} catch (SQLException e) {
			log.error("getAllData SQLException,", e);
		}
		if (updateFieldValue==null){ 
			this.jobPage.put("lastUpdateTime", System.currentTimeMillis()); 
		}else{
			this.jobPage.put("lastUpdateTime", updateFieldValue); 
		}
		this.jobPage.put("maxId", maxId);
		this.jobPage.put("datas", this.datas);
	} 
}