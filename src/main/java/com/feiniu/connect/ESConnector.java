package com.feiniu.connect;

import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;

public final class ESConnector {
	
	private Client client;
	private BulkProcessor bulkProcessor;
	private AtomicBoolean bulkRunState  = new AtomicBoolean(true);
	
	public Client getClient() {
		return client;
	}
	public void setClient(Client client) {
		this.client = client;
	}
	public boolean getRunState() {
		return bulkRunState.get();
	}
	public void setRunState(boolean state) {
		bulkRunState.set(state);
	}
	public BulkProcessor getBulkProcessor() {
		return bulkProcessor;
	}
	public void setBulkProcessor(BulkProcessor bulkProcessor) {
		this.bulkProcessor = bulkProcessor;
	}
	
}
