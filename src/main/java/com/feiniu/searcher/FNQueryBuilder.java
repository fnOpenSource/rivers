package com.feiniu.searcher;

import com.feiniu.model.ESQueryModel;
import com.feiniu.model.FNRequest;

public interface FNQueryBuilder {
	ESQueryModel getQuery(FNRequest request);
}
