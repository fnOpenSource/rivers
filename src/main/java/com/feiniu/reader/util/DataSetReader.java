package com.feiniu.reader.util;

import java.util.HashMap;
import java.util.LinkedList;

import com.feiniu.config.GlobalParam;
import com.feiniu.model.PipeDataUnit; 
/**
 * pass data set in argument,writer will auto get each line
 * @author chengwen
 * @version 1.0
 */
public class DataSetReader{  
	private String IncrementColumn;
	private String keyColumn;
	private String READER_LAST_STAMP = "";
	private String maxId = "";
	private LinkedList<PipeDataUnit> datas;
	private boolean status = true;

	@SuppressWarnings("unchecked") 
	public void init(HashMap<String, Object> rs) {
		if (rs.size() > 2) {
			this.keyColumn =  String.valueOf(rs.get(GlobalParam.READER_KEY));
			this.IncrementColumn = String.valueOf(rs.get(GlobalParam.READER_SCAN_KEY));
			this.maxId = String.valueOf(rs.get("maxId"));
			if(rs.containsKey(GlobalParam.READER_LAST_STAMP))
				this.READER_LAST_STAMP = String.valueOf(rs.get(GlobalParam.READER_LAST_STAMP));
			this.status = (boolean) rs.get(GlobalParam.READER_STATUS);
			this.datas = (LinkedList<PipeDataUnit>) rs.get("datas");
		}
	}
 
	public String getIncrementColumn() {
		return IncrementColumn;
	}
 
	public PipeDataUnit getLineData() {  
		return this.datas.poll();
	}
 
	public boolean nextLine() {
		if (datas.isEmpty()) {
			this.keyColumn=null;
			this.IncrementColumn=null;
			this.datas.clear();
			return false; 
		}
		return true;
	}
 
	public void close() {
		READER_LAST_STAMP = "";
		maxId = "";
		status = true;
		keyColumn = null;
		IncrementColumn = null;
	}
 
	public String getScanStamp() {
		return READER_LAST_STAMP;
	}
  
	public String getMaxId() {
		return maxId;
	}
 
	public String getkeyColumn() { 
		return keyColumn;
	}
 
	public boolean status() { 
		return status;
	} 
}
