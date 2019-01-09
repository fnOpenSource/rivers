package org.elasticflow.param.pipe;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.param.warehouse.WarehouseParam;

public class ConnectParams {
	private String L1Seq; 
	private Object plugin;
	private volatile WarehouseParam whp;
	private volatile InstanceConfig instanceConfig; 

	public String getL1Seq() {
		return L1Seq;
	}

	public void setL1Seq(String l1Seq) {
		L1Seq = l1Seq;
	}

	public WarehouseParam getWhp() {
		return whp;
	}

	public void setWhp(WarehouseParam whp) {
		this.whp = whp;
	}

	public InstanceConfig getInstanceConfig() {
		return instanceConfig;
	}

	public void setInstanceConfig(InstanceConfig instanceConfig) {
		this.instanceConfig = instanceConfig;
	}

	public Object getPlugin() {
		return plugin;
	}

	public void setPlugin(Object plugin) {
		this.plugin = plugin;
	} 
}
