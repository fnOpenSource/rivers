package com.feiniu.service;

import java.util.HashMap;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:23
 */
public interface FNService {
	public void init(HashMap<String, Object> serviceParams);
	public void close();
	public void start();
}
