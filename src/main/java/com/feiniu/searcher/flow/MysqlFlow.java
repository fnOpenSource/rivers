package com.feiniu.searcher.flow;

import java.util.HashMap;

import com.feiniu.model.searcher.SearcherModel;
import com.feiniu.model.searcher.SearcherResult;
import com.feiniu.searcher.SearcherFlowSocket;
import com.feiniu.searcher.handler.Handler;
import com.feiniu.util.FNException;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:23
 */
public class MysqlFlow extends SearcherFlowSocket{

	public static MysqlFlow getInstance(HashMap<String, Object> connectParams) {
		return null;
	}

	@Override
	public SearcherResult Search(SearcherModel<?, ?, ?> query, String instance, Handler handler) throws FNException {
		// TODO Auto-generated method stub
		return null;
	}
}
