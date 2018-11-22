package com.feiniu.task;

import java.util.Map;

import com.feiniu.field.RiverField;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-08 16:49
 */
public class JobPage {
	private String sql;
	private String incrementField;
	private String keyColumn;
	private Map<String, RiverField> transField;
	private long timeStamp = System.currentTimeMillis();
}
