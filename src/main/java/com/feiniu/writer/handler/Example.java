package com.feiniu.writer.handler;

import java.util.HashMap;

import com.feiniu.writer.flow.SolrFlow;

public class Example extends SolrFlow{
 
	public static Example getInstance(HashMap<String, Object> connectParams) {
		Example o = new Example();
		o.INIT(connectParams);
		return o;
	}
}
