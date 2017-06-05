package javaclz.persist.config;

import javaclz.persist.accessor.PersistenceAccessorFactory.DBTYPE;
import java.util.Map;

public class DbAccessorConf extends PersistenceAccessorConf{

	private final String DB_TYPE_KEY = "ADAPTER_DB_TYPE_RESERVED_KEY";
	private final String DB_HOST_KEY = "ADAPTER_DB_HOST_RESERVED_KEY";
	private final String DB_PORT_KEY = "ADAPTER_DB_PORT_RESERVED_KEY";
	private final String DB_NAME_KEY = "ADAPTER_DB_NAME_RESERVED_KEY";
	private final String DB_USER_KEY = "ADAPTER_DB_USER_RESERVED_KEY";
	private final String DB_PASSWARD_KEY = "ADAPTER_DB_PASSWARD_RESERVED_KEY";
	private final String DB_TABLE_KEY = "ADAPTER_DB_TABLE_RESERVED_KEY";
	
	public DbAccessorConf() {
		super();
	}
	
	public DbAccessorConf(Map<String, Object> confs) {
		super(confs);
	}
	
	public void setDbType(DBTYPE dbName) {
		addOrUpdateConf(DB_TYPE_KEY, dbName);
	}
	
	public DBTYPE getDbType() {
		Object obj = getObj(DB_TYPE_KEY);
		if (obj != null) {
			return (DBTYPE)obj;
		} else {
			return DBTYPE.NONE;
		}
	}
	
	public void setDbHost(String dbHost) {
		addOrUpdateConf(DB_HOST_KEY, dbHost);
	}
	
	public String getDbHost() {
		return getString(DB_HOST_KEY);
	}
	
	public void setDbPort(int dbPort) {
		addOrUpdateConf(DB_PORT_KEY, dbPort);
	}
	
	public int getDbPort() {
		Integer port = getInt(DB_PORT_KEY);
		return port == null ? -1 : port;
	}
	
	public void setDbName(String dbName) {
		addOrUpdateConf(DB_NAME_KEY, dbName);
	}
	
	public String getDbName() {
		return getString(DB_NAME_KEY);
	}
	
	public void setUser(String user) {
		addOrUpdateConf(DB_USER_KEY, user);
	}
	
	public String getUser() {
		return getString(DB_USER_KEY);
	}
	
	public void setPassword(String password) {
		addOrUpdateConf(DB_PASSWARD_KEY, password);
	}
	
	public char[] getPassword() {
		String str = getString(DB_PASSWARD_KEY);
		if (str != null) {
			return str.toCharArray();
		} else {
			return null;
		}
	}
	
	public void setTblName(String tblName) {
		addOrUpdateConf(DB_TABLE_KEY, tblName);
	}
	
	public String getTblName() {
		return getString(DB_TABLE_KEY);
	}

}
