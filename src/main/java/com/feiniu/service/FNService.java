package com.feiniu.service;

import java.util.HashMap;


public interface FNService {
	public void init(HashMap<String, Object> serviceParams);
	public void close();
	public void start();
}
