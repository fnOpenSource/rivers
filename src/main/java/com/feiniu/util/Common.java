package com.feiniu.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.KEY_PARAM;
import com.feiniu.config.InstanceConfig;
import com.feiniu.instruction.flow.TransDataFlow;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.node.CPU;

public class Common {
	
	public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public final static Logger LOG = LoggerFactory.getLogger("RIVER");

	private static Set<String> defaultParamSet = new HashSet<String>() {
		private static final long serialVersionUID = 1L;
		{
			add(KEY_PARAM.start.toString());
			add(KEY_PARAM.count.toString());
			add(KEY_PARAM.fl.toString());
			add(KEY_PARAM.facet.toString());
			add(KEY_PARAM.sort.toString());
			add(KEY_PARAM.group.toString());
			add(KEY_PARAM.facet_count.toString());
			add(KEY_PARAM.detail.toString());
		}
	};

	public static boolean isDefaultParam(String p) {
		if (defaultParamSet.contains(p))
			return true;
		else
			return false;
	}

	public static Object getNode2Obj(Node param, Class<?> c) throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, InstantiationException {
		Element element = (Element) param;
		Constructor<?> cons = c.getConstructor();
		Object o = cons.newInstance();

		Field[] fields = c.getDeclaredFields();
		for (int f = 0; f < fields.length; f++) {
			Field field = fields[f];
			String value = null;
			String fieldName = field.getName();
			NodeList list = element.getElementsByTagName(fieldName);

			if (list != null && list.getLength() > 0) {
				Node node = list.item(0);
				value = node.getTextContent();
			} else {
				value = element.getAttribute(fieldName);
			}

			if (param.getNodeName().equals(fieldName)) {
				value = param.getTextContent();
			}

			if (value != null && value.length() > 0) {
				String setMethodName = "set" + field.getName().substring(0, 1).toUpperCase()
						+ field.getName().substring(1);
				Method setMethod = c.getMethod(setMethodName, new Class[] { String.class });
				setMethod.invoke(o, new Object[] { value });
			}
		}
		return o;
	}

	public static List<String> getKeywords(String queryStr) {
		List<String> ret = new ArrayList<String>();
		return ret;
	}

	public static String long2DateFormat(long t) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");
		Date d = new Date(t);
		String ret = sdf1.format(d) + " " + sdf2.format(d);
		return ret;
	}

	public static long getNow() {
		return System.currentTimeMillis() / 1000;
	}

	public static String seconds2time(long second) {
		long h = 0;
		long m = 0;
		long s = 0;
		long temp = second % 3600;
		if (second > 3600) {
			h = second / 3600;
			if (temp != 0) {
				if (temp > 60) {
					m = temp / 60;
					if (temp % 60 != 0) {
						s = temp % 60;
					}
				} else {
					s = temp;
				}
			}
		} else {
			m = second / 60;
			if (second % 60 != 0) {
				s = second % 60;
			}
		}

		String ret = "";
		if (h >= 10)
			ret += h + "h";
		else if (h > 0)
			ret += "0" + h + "h";

		if (m >= 10)
			ret += m + "m";
		else if (m > 0)
			ret += "0" + m + "m";

		if (s >= 10)
			ret += s + "s";
		else if (s >= 0)
			ret += "0" + s + "s";

		return ret;
	}

	public static List<String> String2List(String str, String seperator) {
		List<String> ret = new ArrayList<String>();
		if (str == null || str.length() <= 0)
			return ret;

		String ss[] = str.split(seperator);
		for (String s : ss) {
			if (s.length() > 0)
				ret.add(s);
		}
		return ret;
	}

	public static <T> String List2String(List<T> list, String seperator) {
		StringBuffer ret = new StringBuffer("");
		if (list == null || list.size() <= 0)
			return ret.toString();

		for (int i = 0; i < list.size(); i++) {
			if (i > 0)
				ret.append(seperator);
			ret.append(list.get(i));
		}
		return ret.toString();
	}

	/**
	 * seq for split data
	 * 
	 * @param indexname
	 * @param seq,for
	 *            series data source fetch
	 * @return
	 */
	public static String getTaskStorePath(String instanceName, String seq,String location) {
		return GlobalParam.CONFIG_PATH + "/" + instanceName + "/" + ((seq != null && seq.length() > 0) ? seq + "/" : "")
				+location;
	}

	public static void saveTaskInfo(String instanceName, String seq, String storeId,String location) {
		if (storeId.length() > 0) {
			ZKUtil.setData(getTaskStorePath(instanceName, seq,location),
					storeId + GlobalParam.JOB_STATE_SPERATOR + GlobalParam.LAST_UPDATE_TIME.get(instanceName,seq));
		}
	} 

	/**
	 * 
	 * @param seq
	 *            for series data source sequence
	 * @param instanceName
	 *            data source main tag name
	 * @return String
	 */
	public static String getStoreName(String instanceName, String seq) {
		if (seq != null && seq.length() > 0) {
			return instanceName + "_" + seq;
		} else {
			return instanceName;
		}

	}

	/**
	 * @param seq
	 * 			  for get uniform resource but not store name
	 * @param seq
	 *            for series data source sequence
	 * @param instanceName
	 *            data source main tag name
	 * @param fixName instance name is fixed will not auto generate instance name and ab change will limit
	 * @return String
	 */
	public static String getInstanceName(String instanceName, String seq,String fixName) {
		if(fixName!=null)
			return fixName;
		if (seq != null && seq.length()>0) {
			return instanceName + seq;
		} else {
			return instanceName + GlobalParam.DEFAULT_RESOURCE_SEQ;
		} 
	}
	
	public static String getResourceTag(String instance,String seq,String tag) {
		if (seq != null && seq.length()>0) {
			return instance + seq+tag;
		} else {
			return instance + GlobalParam.DEFAULT_RESOURCE_SEQ+tag;
		} 
	}
	
	public static String getFullStartInfo(String instanceName, String seq) {
		String info = null;
		String path = Common.getTaskStorePath(instanceName, seq,GlobalParam.JOB_FULLINFO_PATH);
		byte[] b = ZKUtil.getData(path,true); 
		if (b != null && b.length > 0) {
			String str = new String(b); 
			if (str.length() > 1) {
				info = str;
			}
		}
		return info;
	}

	/**
	 * get increment store tag name and will auto create new one with some conditions.
	 * 
	 * @param isIncrement
	 * @param reCompute
	 *            force to get storeid recompute from destination engine
	 * @param seq
	 *            for series data source sequence
	 * @param instanceName
	 *            data source main tag name
	 * @return String
	 */
	public static String getStoreId(String instanceName, String seq, TransDataFlow writer, boolean isIncrement,
			boolean reCompute) {
		if (isIncrement) {
			String path = Common.getTaskStorePath(instanceName, seq,GlobalParam.JOB_INCREMENTINFO_PATH);
			byte[] b = ZKUtil.getData(path,true);
			String storeId = "";
			if (b != null && b.length > 0) {
				String str = new String(b);
				String[] strs = str.split(GlobalParam.JOB_STATE_SPERATOR);
				if (strs.length > 0) {
					if (strs[0].equals("a") || strs[0].equals("b")) {
						storeId = strs[0];
					} else {
						storeId = "";
					}
				}
				if (strs.length > 1) {
					GlobalParam.LAST_UPDATE_TIME.set(instanceName,seq, strs[1]);
				}
			}
			if (storeId.length() == 0 || reCompute) {
				storeId = (String) CPU.RUN(writer.getID(), "Pond", "getNewStoreId",false, instanceName, true, seq); 
				if (storeId == null)
					storeId = "a";
				saveTaskInfo(instanceName, seq, storeId,GlobalParam.JOB_INCREMENTINFO_PATH);
			}
			return storeId;
		} else {
			return  (String) CPU.RUN(writer.getID(), "Pond", "getNewStoreId",true, instanceName, false, seq);
		}
	}
	
	/**
	 * get store tag name
	 * @param instanceName
	 * @param seq
	 * @return String
	 */
	public static String getStoreId(String instanceName, String seq) {
		String path = Common.getTaskStorePath(instanceName, seq,GlobalParam.JOB_INCREMENTINFO_PATH);
		byte[] b = ZKUtil.getData(path, true);
		String storeId = "";
		if (b != null && b.length > 0) {
			String str = new String(b);
			String[] strs = str.split(GlobalParam.JOB_STATE_SPERATOR);
			if (strs.length > 0) {
				if (strs[0].equals("a") || strs[0].equals("b")) {
					storeId = strs[0];
				}
			}
		}
		return storeId;
	}
	
	/**
	 * get read data source seq flags
	 * @param instanceName
	 * @param instanceConfig
	 * @return
	 */
	public static List<String> getSeqs(InstanceConfig instanceConfig){
		List<String> seqs = new ArrayList<>();
		WarehouseParam whParam;
		if(GlobalParam.nodeConfig.getNoSqlParamMap().get(instanceConfig.getPipeParam().getDataFrom())!=null){
			whParam = GlobalParam.nodeConfig.getNoSqlParamMap().get(
					instanceConfig.getPipeParam().getDataFrom());
		}else{
			whParam = GlobalParam.nodeConfig.getSqlParamMap().get(
					instanceConfig.getPipeParam().getDataFrom());
		}
		if (null != whParam) {
			seqs = whParam.getSeq();
		}  
		return seqs;
	} 
	
	/**
	 * 
	 * @param heads
	 * @param instanceName
	 * @param storeId
	 * @param seq table seq
	 * @param total
	 * @param maxId
	 * @param lastUpdateTime
	 * @param useTime
	 * @param types
	 * @param moreinfo
	 */
	
	public static String formatLog(String heads,String instanceName, String storeId,
			String seq, String total, String maxId, String lastUpdateTime,
			long useTime, String types, String moreinfo) {
		String useTimeFormat = Common.seconds2time(useTime);
		StringBuffer str = new StringBuffer("["+heads+" "+instanceName + "_" + storeId+"] "+(!seq.equals("") ? " table:" + seq : ""));
		String update;
		if(lastUpdateTime.length()>9 && lastUpdateTime.matches("[0-9]+")){ 
			update = SDF.format(lastUpdateTime.length()<12?new Long(lastUpdateTime+"000"):new Long(lastUpdateTime));
		}else{
			update = lastUpdateTime;
		} 
		switch (types) {
		case "complete":
			str.append(" docs:" + total	+ " " + " useTime: " + useTimeFormat + "}");
			break;
		case "start": 
			str.append(" lastUpdate:" + update);
			break;
		default:
			str.append(" docs:" + total+ (maxId.equals("0") ? "" : " MaxId:" + maxId)
			+ " lastUpdate:" + update + " useTime:"	+ useTimeFormat);
			break;
		} 
		return str.append(moreinfo).toString();
	}
}
