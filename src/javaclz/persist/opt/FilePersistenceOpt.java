package javaclz.persist.opt;

public class FilePersistenceOpt implements PersistenceOpt {

	private final String basePath;
	private final String filePath;

	public FilePersistenceOpt(String basePath, String filePath) {
		this.basePath = basePath;
		this.filePath = filePath;
	}

	// basePath
	public String getStr1() {
		return basePath;
	}

	// tableName
	public String getStr2() {
		return filePath;
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
