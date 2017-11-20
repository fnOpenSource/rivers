package com.feiniu.connect;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;

public class ESConnector {
	private Client client;
	private BulkProcessor bulkProcessor;
	public Client getClient() {
		return client;
	}
	public void setClient(Client client) {
		this.client = client;
	}
	public BulkProcessor getBulkProcessor() {
		return bulkProcessor;
	}
	public void setBulkProcessor(BulkProcessor bulkProcessor) {
		this.bulkProcessor = bulkProcessor;
	}
	
}
