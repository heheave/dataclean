package javaclz.persist.config;

import java.util.Map;

public class FileAccessorConf extends PersistenceAccessorConf{

	private final String FILE_DATE_FORMATE_KEY = "ADAPTER_FILE_DATE_FORMATE_RESERVED_KEY";
	
	public FileAccessorConf() {
		super();
	}
	
	public FileAccessorConf(Map<String, Object> confs) {
		super(confs);
	}
	
	public void setDateFormate(String dbName) {
		addOrUpdateConf(FILE_DATE_FORMATE_KEY, dbName);
	}
	
	public String getDateFormate() {
		return getString(FILE_DATE_FORMATE_KEY);
	}
}
