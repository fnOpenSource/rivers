package com.feiniu.writer.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.InstanceConfig;
import com.feiniu.field.RiverField;
import com.feiniu.model.reader.PipeDataUnit;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.util.SqlUtil;
import com.feiniu.writer.WriterFlowSocket;

public class MysqlFlow extends WriterFlowSocket {

	private final static Logger log = LoggerFactory.getLogger("MysqlFlow");

	public static MysqlFlow getInstance(HashMap<String, Object> connectParams) {
		MysqlFlow o = new MysqlFlow();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public void write(String keyColumn, PipeDataUnit unit, Map<String, RiverField> transParams, String instance,
			String storeId, boolean isUpdate) throws FNException {
		String table = Common.getStoreName(instance, storeId);
		boolean releaseConn = false;
		try { 
			PREPARE(false, false);
			if (!ISLINK())
				return;
			Connection conn = (Connection) GETSOCKET().getConnection(false);
			try (PreparedStatement statement = conn.prepareStatement(SqlUtil.getWriteSql(table, unit, transParams));) {
				statement.execute();
			} catch (Exception e) {
				log.error("PreparedStatement Exception", e);
			}
		} catch (Exception e) {
			log.error("write Exception", e);
		} finally {
			REALEASE(false, releaseConn);
		}
	}

	@Override
	public void flush() throws Exception {

	}

	@Override
	public String getNewStoreId(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		String select = "b";
		boolean releaseConn = false;
		PREPARE(false, false);
		if (!ISLINK())
			return select;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		String checkSql = "SELECT table_name FROM information_schema.TABLES WHERE table_name ='"
				+ Common.getStoreName(mainName, "a") + "';";
		try (PreparedStatement statement = conn.prepareStatement(checkSql);) {
			try (ResultSet rs = statement.executeQuery();) {
				if (rs.getRow() == 0)
					select = "a";
			} catch (Exception e) {
				log.error("ResultSet Exception", e);
			}
		} catch (Exception e) {
			log.error("PreparedStatement Exception", e);
		} finally {
			REALEASE(false, releaseConn);
		}
		return select;
	}

	@Override
	public boolean create(String instance, String storeId, Map<String, RiverField> transParams) {
		String name = Common.getStoreName(instance, storeId);
		String type = instance;
		boolean releaseConn = false;
		PREPARE(false, false);
		if (!ISLINK())
			return false;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		try (PreparedStatement statement = conn.prepareStatement(getTableSql(name, transParams));) {
			log.info("create Instance " + name + ":" + type);
			statement.execute();
			return true;
		} catch (Exception e) {
			log.error("create Instance " + name + ":" + type + " failed!", e);
			return false;
		} finally {
			REALEASE(false, releaseConn);
		}
	}

	private String getTableSql(String instance, Map<String, RiverField> transParams) {
		StringBuffer sf = new StringBuffer();
		sf.append("create table " + instance + " (");
		for (Map.Entry<String, RiverField> e : transParams.entrySet()) {
			RiverField p = e.getValue();
			if (p.getName() == null)
				continue;
			sf.append(p.getAlias());
			sf.append(" " + p.getIndextype());
			sf.append(" ,");
			if (p.getIndexed() == "true") {
				sf.append("KEY `" + p.getName() + "` (`" + p.getName() + "`) USING BTREE ");
			}
		}
		String tmp = sf.substring(0, sf.length() - 1);
		return tmp + ");";
	}
}
