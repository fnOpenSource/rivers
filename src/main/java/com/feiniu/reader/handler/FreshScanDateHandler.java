package com.feiniu.reader.handler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.feiniu.config.GlobalParam;

public class FreshScanDateHandler implements Handler {

	@SuppressWarnings("unchecked")
	@Override
	public String Handle(Object... args) {
		if (args.length == 2) {
			HashMap<String, String> Params;
			Params = (HashMap<String, String>) args[1];
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				if (Params.get(GlobalParam._start_time).length()<5) {
					Params.put(GlobalParam._start_time, sdf.parse("2017-11-01").getTime() + "");
				} else {
					Date date = new Date(new Long(Params.get(GlobalParam._start_time)));
					String day = sdf.format(date);
					Params.put(GlobalParam._start_time, sdf.parse(day).getTime() + "");
				}  
				Params.put(GlobalParam._end_time, (Long.valueOf(Params.get(GlobalParam._start_time))+3600*24*1000)+"");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return (String) args[0];
	}

	@Override
	public boolean needLoop(HashMap<String, String> params) {
		if(Long.valueOf(params.get(GlobalParam._end_time))>System.currentTimeMillis()) {
			return false;
		}else {
			params.put(GlobalParam._start_time, (Long.valueOf(params.get(GlobalParam._start_time))+3600*24*1000) + "");
			params.put(GlobalParam._end_time, (Long.valueOf(params.get(GlobalParam._start_time))+3600*24*1000) + "");
			return true;
		} 
	}

}
