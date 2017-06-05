package javaclz.persist.opt;

import java.io.Serializable;

// can store five string values
public interface PersistenceOpt extends Serializable{
	
	public String getStr1();

	public String getStr2();

	public String getStr3();

	public String getStr4();

	public String getStr5();

	public PersistenceOpt backupOpt();
}
