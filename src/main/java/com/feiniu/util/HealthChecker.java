package com.feiniu.util;

import org.springframework.beans.factory.annotation.Value;

public class HealthChecker {
	@Value("#{chechksrvConfig['checksrv.version']}")
	private String version;
	
	public String getCheckInfo(){
		return "version:" + version + "\nstatus:ok"; 
	}
}
