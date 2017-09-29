package com.feiniu.reader.flow;

import java.util.HashMap;
import java.util.LinkedList;

import com.feiniu.model.WriteUnit;
/**
 * pass data set in argument,writer will auto get each line
 * @author chengwen
 * @version 1.0
 */
public class DataSetReader implements Reader<HashMap<String, Object>> {  
	private String IncrementColumn;
	private String keyColumn;
	private String lastUpdateTime = "";
	private String maxId = "";
	private LinkedList<WriteUnit> datas;  

	@SuppressWarnings("unchecked")
	@Override
	public void init(HashMap<String, Object> rs) {
		if (rs.size() > 2) {
			this.keyColumn =  String.valueOf(rs.get("keyColumn"));
			this.IncrementColumn = String.valueOf(rs.get("IncrementColumn"));
			this.maxId = String.valueOf(rs.get("maxId"));
			if(rs.containsKey("lastUpdateTime"))
				this.lastUpdateTime = String.valueOf(rs.get("lastUpdateTime"));
			this.datas = (LinkedList<WriteUnit>) rs.get("datas");
		}
	}

	@Override
	public String getIncrementColumn() {
		return IncrementColumn;
	}

	@Override
	public WriteUnit getLineData() {  
		return this.datas.poll();
	}

	@Override
	public boolean nextLine() {
		if (datas.isEmpty()) {
			this.keyColumn=null;
			this.IncrementColumn=null;
			this.datas.clear();
			return false; 
		}
		return true;
	}

	@Override
	public void close() {
	}

	@Override
	public String getLastUpdateTime() {
		return lastUpdateTime;
	}

	@Override
	public String getMaxId() {
		return maxId;
	}

	@Override
	public String getkeyColumn() { 
		return keyColumn;
	}

}
