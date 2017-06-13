package javaclz.persist.accessor;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import javaclz.persist.config.DbAccessorConf;
import javaclz.persist.config.FileAccessorConf;
import javaclz.persist.config.PersistenceAccessorConf;

import java.io.IOException;
import java.util.Arrays;

public class PersistenceAccessorFactory {
	
	public enum DBTYPE{
		NONE, MONGO, MYSQL, SQLSERVER
	}
	
	private PersistenceAccessorFactory(){}
	
	public static PersistenceAccessor getAccessor(PersistenceAccessorConf pac){
		PersistenceAccessor pa = null;
		if (pac instanceof DbAccessorConf) {
			DbAccessorConf dac = (DbAccessorConf)pac;
			DBTYPE dbType = dac.getDbType();
			switch(dbType) {
			case NONE: {
				break;
			}
			case MONGO:{
				pa = getMongoAccessor(dac);
				break;
			}
			default:
				pa = null;
			}
		} else if (pac instanceof FileAccessorConf){
			FileAccessorConf fac = (FileAccessorConf)pac;
			pa = getFileAccessor(fac);		
		} else {
			pa = null;
		}
		
		return pa;
	}
	
	private static MongoAccessor getMongoAccessor(DbAccessorConf dac) {
		String DB_NAME = dac.getDbName();
		String DB_USER = dac.getUser();
		char[] DB_PASSWORD = dac.getPassword();
		String DB_HOST = dac.getDbHost();
		int DB_PORT= dac.getDbPort();
		if (DB_HOST == null || DB_PORT < 0) {
			return null;
		}

		ServerAddress serverAddress = new ServerAddress(DB_HOST, DB_PORT);
		if (DB_USER == null || DB_PASSWORD == null || DB_NAME == null) {
			MongoClient mc = new MongoClient(serverAddress);
			return new MongoAccessor(mc);
		}

		MongoCredential credential = MongoCredential.createCredential(DB_USER, DB_NAME,
				DB_PASSWORD);
		MongoClient mc = new MongoClient(serverAddress, Arrays.asList(credential));
		return new MongoAccessor(mc);
	}
	
	private static FileAccessor getFileAccessor(FileAccessorConf fac) {
		String DATE_FORMAT = fac.getDateFormat();
		if (DATE_FORMAT == null) {
			return null;
		}
		try {
			return new FileAccessor(fac.getConfiguration(), DATE_FORMAT);
		} catch (IOException e) {
			e.printStackTrace();
			return  null;
		}

	}
	
}


