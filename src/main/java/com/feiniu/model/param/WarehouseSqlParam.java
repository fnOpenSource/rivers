package com.feiniu.model.param;

import java.util.ArrayList;
import java.util.List;

import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.util.Common;

public class WarehouseSqlParam implements WarehouseParam{
	
	private String name = "";
	private String alias;
	private String host = "";
	private int port;
	private String dbname = "";
	private String user = "";
	private String password = "";
	private DATA_TYPE type = DATA_TYPE.UNKNOWN;
	private List<String> seq = new ArrayList<String>();
	private String handler;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = Integer.valueOf(port);
	}
	@Override
	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	public String getDbname() {
		return dbname;
	}
	public void setDbname(String db) {
		this.dbname = db;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public DATA_TYPE getType() {
		return this.type;
	}
	public void setType(String type) {
		if (type.equalsIgnoreCase("MYSQL"))
			this.type = DATA_TYPE.MYSQL;
		else if (type.equalsIgnoreCase("ORACLE"))
			this.type = DATA_TYPE.ORACLE;
		else if (type.equalsIgnoreCase("HIVE"))
			this.type = DATA_TYPE.HIVE;
	}
	
	public String getAlias() {
		if(this.alias==null){
			this.alias = this.name;
		}
		return this.alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	@Override
	public List<String> getSeq() {
		return this.seq;
	}
	
	@Override
	public void setSeq(String seqs) {
		this.seq = Common.String2List(seqs, ",");
	}
	@Override
	public String getPoolName(String seq) {  
		return this.alias+"_"+this.type+"_"+this.host+"_"+((seq != null) ? this.dbname.replace("#{seq}", seq):this.dbname);
	}	
}
