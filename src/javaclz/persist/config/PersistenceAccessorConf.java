package javaclz.persist.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class PersistenceAccessorConf {
	
	private final Map<String, Object> confs = new HashMap<String, Object>();
	
	public PersistenceAccessorConf() {}
	
	public PersistenceAccessorConf(Map<String, Object> confs) {
		if (confs!=null){
			for (Entry<String, Object> entry: confs.entrySet()) {
				this.confs.put(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public Object getObj(String key) {
		return confs.get(key);
	}
	
	public void addOrUpdateConf(String key, Object value) {
		confs.put(key, value);
	}
	
	public String getString(String key) {
		Object obj = confs.get(key);
		if (obj!=null){
			return obj.toString();
		} else {
			return null;
		}
	}
	
	public Integer getInt(String key) {
		Object obj = confs.get(key);
		Integer result;
		if (obj!=null){
			try {
				result = Integer.parseInt(obj.toString());
			} catch(Exception e) {
				result = null;
			}
		} else {
			result =  null;
		}
		return result;
	}
	
	public Long getLong(String key) {
		Object obj = confs.get(key);
		Long result;
		if (obj!=null){
			try {
				result = Long.parseLong(obj.toString());
			} catch(Exception e) {
				result = null;
			}
		} else {
			result =  null;
		}
		return result;
	}
	
	public Float getFloat(String key) {
		Object obj = confs.get(key);
		Float result;
		if (obj!=null){
			try {
				result = Float.parseFloat(obj.toString());
			} catch(Exception e) {
				result = null;
			}
		} else {
			result =  null;
		}
		return result;
	}
	
	public Double getDouble(String key) {
		Object obj = confs.get(key);
		Double result;
		if (obj!=null){
			try {
				result = Double.parseDouble(obj.toString());
			} catch(Exception e) {
				result = null;
			}
		} else {
			result =  null;
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		int size = confs.size();
		int idx = 0;
		for(String key : confs.keySet()) {
			sb.append(key);
			sb.append(':');
			sb.append(confs.get(key).toString());
			if (idx != size - 1) {
				sb.append(',');
			}
			idx++;
		}	
		return sb.toString();
	}

	
}
