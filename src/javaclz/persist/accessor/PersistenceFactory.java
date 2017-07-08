package javaclz.persist.accessor;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import javaclz.persist.config.DbAccessorConf;
import javaclz.persist.config.FileAccessorConf;
import javaclz.persist.config.PersistenceAccessorConf;

import java.io.IOException;
import java.util.Arrays;

public class PersistenceFactory {
	
	public enum DBTYPE{
		NONE, MONGO, MYSQL, SQLSERVER
	}
	
	private PersistenceFactory(){}
	
	public static ModulePersistence getAccessor(PersistenceAccessorConf pac){
		ModulePersistence pa = null;
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
	
	private static MongoPersistence getMongoAccessor(DbAccessorConf dac) {
		String DB_NAME = dac.getDbName();
		String DB_USER = dac.getUser();
		char[] DB_PASSWORD = dac.getPassword();
		String DB_HOST = dac.getDbHost();
		int DB_PORT= dac.getDbPort();
		if (DB_HOST == null || DB_PORT < 0) {
			return null;
		}
		MongoClientOptions.Builder builder;
		Integer timeout = dac.getTimeout();
		if (timeout != null) {
			builder = new MongoClientOptions.Builder();
            builder.serverSelectionTimeout(timeout);
			builder.connectTimeout(timeout);
			//builder.socketTimeout(timeout);
		} else {
			builder = new MongoClientOptions.Builder();
		}

		ServerAddress serverAddress = new ServerAddress(DB_HOST, DB_PORT);
		if (DB_USER == null || DB_PASSWORD == null || DB_NAME == null) {
			MongoClient mc = new MongoClient(serverAddress, builder.build());
			return new MongoPersistence(mc);
		}

		MongoCredential credential = MongoCredential.createCredential(DB_USER, DB_NAME,
				DB_PASSWORD);

		MongoClient mc = new MongoClient(serverAddress, Arrays.asList(credential), builder.build());
		return new MongoPersistence(mc);
	}
	
	private static FilePersistence getFileAccessor(FileAccessorConf fac) {
		String DATE_FORMAT = fac.getDateFormat();
		if (DATE_FORMAT == null) {
			return null;
		}
		try {
			return new FilePersistence(fac.getConfiguration(), DATE_FORMAT);
		} catch (IOException e) {
			e.printStackTrace();
			return  null;
		}

	}
	
}


