package com.feiniu.reader.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil; 
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.connect.FnConnection;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.TransParam;
import com.feiniu.reader.handler.Handler;

public class HbaseFlow extends ReaderFlowSocket<HashMap<String, Object>> { 
	 
	private final static Logger log = LoggerFactory.getLogger(HbaseFlow.class);

	public static HbaseFlow getInstance(HashMap<String, Object> connectParams) {
		HbaseFlow o = new HbaseFlow();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		String tableColumnFamily = (String) this.connectParams
				.get("defaultValue");
		if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
			String[] strs = tableColumnFamily.split(":");
			if (strs != null && strs.length > 0)
				this.connectParams.put("tableName", strs[0]);
			if (strs != null && strs.length > 1)
				this.connectParams.put("columnFamily", strs[1]);
		}
		this.poolName = String.valueOf(connectParams.get("poolName")); 
	}

	@Override
	public HashMap<String, Object> getJobPage(HashMap<String, String> param,Map<String, TransParam> transParams,Handler handler) {
		try {
			while (isLocked.get()) {
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			log.error("getJobPage Thread Exception",e);
			return null;
		}
		isLocked.set(true);
		FnConnection<?> FC = LINK(false);
		this.jobPage.clear();
		boolean releaseConn = false;
		try {
			Table conn = (Table) FC.getConnection(false);
			Scan scan = new Scan();
			List<Filter> filters = new ArrayList<Filter>();
			SingleColumnValueFilter range = new SingleColumnValueFilter(
					Bytes.toBytes(this.connectParams.get("columnFamily")
							.toString()), Bytes.toBytes(param
							.get(GlobalParam._incrementField)),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(param.get("startTime"))));
			range.setLatestVersionOnly(true);
			range.setFilterIfMissing(true);
			filters.add(range);
			scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters));
			scan.setStartRow(Bytes.toBytes(param.get(GlobalParam._start)));
			scan.setStopRow(Bytes.toBytes(param.get(GlobalParam._end)));
			scan.setCaching(GlobalParam.MAX_PER_PAGE);
			scan.addFamily(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()));
			ResultScanner resultScanner = conn.getScanner(scan);
			try {   
				String maxId = null;
				String updateFieldValue=null;
				this.datas.clear();
				this.jobPage.put(GlobalParam.READER_KEY, param.get(GlobalParam.READER_KEY));
				this.jobPage.put(GlobalParam.READER_SCAN_KEY, param.get(GlobalParam.READER_SCAN_KEY)); 
				for (Result r : resultScanner) { 
					WriteUnit u = WriteUnit.getInstance();
					if(handler==null){
						for (Cell cell : r.rawCells()) {
							String k = new String(CellUtil.cloneQualifier(cell));
							String v = new String(CellUtil.cloneValue(cell), "UTF-8"); 
							if(k.equals(this.jobPage.get(GlobalParam.READER_KEY))){
								u.setKeyColumnVal(v);
								maxId = v;
							}
							if(k.equals(this.jobPage.get(GlobalParam.READER_SCAN_KEY))){
								updateFieldValue = v;
							}
							u.addFieldValue(k, v, transParams);
						} 
					}else{
						handler.Handle(r,u);
					} 
					this.datas.add(u);
				} 
				if (updateFieldValue==null){ 
					this.jobPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis()); 
				}else{
					this.jobPage.put(GlobalParam.READER_LAST_STAMP, updateFieldValue); 
				}
				this.jobPage.put("maxId", maxId);
				this.jobPage.put("datas", this.datas);
			} catch (Exception e) {
				this.jobPage.put(GlobalParam.READER_LAST_STAMP, -1);
				log.error("SqlReader init Exception", e);
			} 
		} catch (Exception e) {
			releaseConn = true;
			log.error("getJobPage Exception", e);
		}finally{
			UNLINK(FC,releaseConn);
		} 
		return this.jobPage;
	}

	@Override
	public List<String> getPageSplit(HashMap<String, String> param) {
		int i = 0;
		FnConnection<?> FC = LINK(false);
		Table conn = (Table) FC.getConnection(false);
		List<String> dt = new ArrayList<String>();
		boolean releaseConn = false;
		try {
			Scan scan = new Scan();
			List<Filter> filters = new ArrayList<Filter>();
			SingleColumnValueFilter range = new SingleColumnValueFilter(
					Bytes.toBytes(this.connectParams.get("columnFamily")
							.toString()), Bytes.toBytes(param
							.get(GlobalParam._incrementField)),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(param.get("startTime"))));
			range.setLatestVersionOnly(true);
			range.setFilterIfMissing(true);
			filters.add(range);
			scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters));
			scan.setCaching(GlobalParam.MAX_PER_PAGE);
			scan.addFamily(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()));
			scan.addColumn(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()), Bytes.toBytes(param
					.get(GlobalParam._incrementField)));
			scan.addColumn(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()), Bytes.toBytes(param.get("column")));
			ResultScanner resultScanner = conn.getScanner(scan);
			for (Result r : resultScanner) {
				if (i % GlobalParam.MAX_PER_PAGE == 0) {
					dt.add(Bytes.toString(r.getRow()));
				}
				i += r.size();
			}
		} catch (Exception e) {
			releaseConn = true;
			log.error("getPageSplit Exception", e);
		}finally{ 
			UNLINK(FC,releaseConn);
		}
		return dt;
	}
 

}
