package com.feiniu.util;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtil {

	private static final int CONNECTION_TIMEOUT = 50000;
	private final static CountDownLatch connectedSemaphore = new CountDownLatch(
			1);
	private static String zkHost = null;
	private static ZooKeeper zk = null;
	private static Watcher watcher = null;
	private final static Logger log = LoggerFactory.getLogger(ZKUtil.class);

	private static ZooKeeper getZk() { 
		synchronized (ZKUtil.class) {
			if (zk == null || zk.getState().equals(States.CLOSED)) {
				connection();
			}
		} 
		return zk;
	}

	private static void connection() {
		try {
			watcher = new Watcher() {
				public void process(WatchedEvent event) {
					connectedSemaphore.countDown();
				}
			};
			zk = new ZooKeeper(zkHost, CONNECTION_TIMEOUT, watcher);
			connectedSemaphore.await();
		} catch (Exception e) {
			log.error("connection Exception", e);
		}
	}

	public static void setZkHost(String zkString) {
		zkHost = zkString;
	}

	public static void setData(String filename, String Content) {
		byte[] bt = Content.getBytes();
		try {
			getZk().setData(filename, bt, -1);
		} catch (Exception e) {
			log.error("setData Exception", e);
		}

	}

	public static byte[] getData(String filename) {
		try {
			return getZk().getData(filename, watcher, null);
		} catch (Exception e) {
			log.error("getData Exception", e);
			return null;
		}

	}
}
