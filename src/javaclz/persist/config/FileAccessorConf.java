package javaclz.persist.config;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class FileAccessorConf extends PersistenceAccessorConf{

	private static final String FILE_DATE_FORMATE_KEY = "PERSIST_FILE_DATE_FORMATE_RESERVED_KEY";

	private final Configuration configuration;

	public FileAccessorConf(Configuration configuration) {
		super();
		this.configuration = configuration;
	}
	
	public FileAccessorConf(Configuration configuration, Map<String, Object> confs) {
		super(confs);
		this.configuration = configuration;
	}
	
	public void setDateFormat(String dbName) {
		addOrUpdateConf(FILE_DATE_FORMATE_KEY, dbName);
	}

	public String getDateFormat() {
		return getString(FILE_DATE_FORMATE_KEY);
	}

	public Configuration getConfiguration() {
		return configuration;
	}
}
