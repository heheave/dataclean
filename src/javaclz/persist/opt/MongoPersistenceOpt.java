package javaclz.persist.opt;

public class MongoPersistenceOpt implements PersistenceOpt {

	private final String dbName;

	private final String tableName;

	//private final String

	public MongoPersistenceOpt(String dbName, String tblName) {
		this.dbName = dbName;
		this.tableName = tblName;
	}

	// dbName
	public String getStr1() {
		return dbName;
	}

	// tableName
	public String getStr2() {
		return tableName;
	}

	// unused
	public String getStr3() {
		return null;
	}

	// unused
	public String getStr4() {
		return null;
	}

	// unused
	public String getStr5() {
		return null;
	}

	public PersistenceOpt backupOpt() {
		return null;
	}
}
