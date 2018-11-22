package com.feiniu.instruction.sets;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.instruction.Context;
import com.feiniu.instruction.Instruction;
import com.feiniu.node.CPU;

/**
 *  * runtime manage
 * @author chengwen
 * @version 2.1
 * @date 2018-11-02 16:47
 */
public class Track extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Track");

	static HashMap<String, HashMap<String, Object>> tmpStore = new HashMap<>();

	public static boolean cpuPrepare(Context context, Object[] args) {
		if (context != null)
			return true;
		String seq = null;
		String instance;
		String id;
		if (args.length == 2) {
			instance = (String) args[0];
			id = (String) args[1];
		} else if (args.length == 3) {
			instance = (String) args[0];
			seq = (String) args[1];
			id = (String) args[2];
		} else {
			return false;
		}
		CPU.prepare(id, GlobalParam.nodeConfig.getInstanceConfigs().get(instance),
				GlobalParam.SOCKET_CENTER.getWriterSocket(
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(), instance,
						seq, ""),
				GlobalParam.SOCKET_CENTER.getReaderSocket(
						GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadFrom(), instance,
						seq, ""));
		return true;

	}

	public static boolean cpuFree(Context context, Object[] args) {
		if (isValid(1, args)) {
			String id = (String) args[0];
			if (tmpStore.containsKey(id)) {
				tmpStore.remove(id);
			}
		}
		return true;
	}

	/**
	 * @param args
	 *            parameter order is: String key,Object val
	 */
	public static void store(Context context, Object[] args) {
		if (isValid(3, args)) {
			String key = (String) args[0];
			Object val = args[1];
			String id = (String) args[2];
			if (!tmpStore.containsKey(id)) {
				tmpStore.put(id, new HashMap<String, Object>());
			}
			tmpStore.get(id).put(key, val);
		} else {
			log.error("store parameter not match!");
		}
	}

	/**
	 * @param args
	 *            parameter order is: String key
	 */
	public static Object fetch(Context context, Object[] args) {
		if (isValid(2, args)) {
			String key = (String) args[0];
			String id = (String) args[1];
			if (tmpStore.containsKey(id)) {
				return tmpStore.get(id).get(key);
			}
		} else {
			log.error("fetch parameter not match!");
		}
		return null;
	}
}
