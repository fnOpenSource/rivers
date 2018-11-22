package com.feiniu.reader.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.field.RiverField;
import com.feiniu.model.reader.DataPage;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.reader.handler.Handler;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:33
 */
public class FileFlow extends ReaderFlowSocket {
	
	private final static Logger log = LoggerFactory.getLogger(FileFlow.class);
	
	public static FileFlow getInstance(HashMap<String, Object> connectParams) {
		FileFlow o = new FileFlow();
		o.INIT(connectParams);
		return o;
	}
	

	@Override
	public DataPage getPageData(HashMap<String, String> param, Map<String, RiverField> transParams, Handler handler,
			int pageSize) {
		PREPARE(false,false);
		boolean releaseConn = false;
		try {
			if(!ISLINK())
				return this.dataPage;
		} catch (Exception e) {
			releaseConn = true;
			log.error("get dataPage Exception", e);
		}finally{
			REALEASE(false,releaseConn);
		} 
		return this.dataPage;
	}

	@Override
	public List<String> getPageSplit(HashMap<String, String> param, int pageSize) {
		List<String> dt = new ArrayList<String>(); 
		PREPARE(false,false);
		if(!ISLINK())
			return dt; 
		boolean releaseConn = false;
		try {
			
		} catch (Exception e) {
			releaseConn = true;
			log.error("getPageSplit Exception", e);
		}finally{ 
			REALEASE(false,releaseConn);
		}
		return dt;
	} 

}
