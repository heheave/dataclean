package javaclz.persist;

import javaclz.JavaV;
import javaclz.persist.accessor.PersistenceAccessor;
import javaclz.persist.accessor.PersistenceAccessorFactory;
import javaclz.persist.config.DbAccessorConf;
import javaclz.persist.data.PersistenceData;
import javaclz.persist.opt.PersistenceOpt;
import org.apache.log4j.Logger;
import javaclz.persist.accessor.PersistenceAccessorFactory.DBTYPE;
import java.util.Collection;
import java.util.Properties;

public class AdapterPersistence implements Persistence {

	private static final Logger log = Logger.getLogger(AdapterPersistence.class);

	private final Properties conf;

	private final DbAccessorConf mongDbConf;

	//private final FileAccessorConf fileConf;

	private PersistenceAccessor dbPa;

	private PersistenceAccessor filePa;

	public AdapterPersistence(Properties conf) {
		this.conf = conf;
		mongDbConf = new DbAccessorConf();
		mongDbConf.setDbType(DBTYPE.MONGO);
		String host = conf.getProperty(JavaV.MASTER_HOST, "localhost");
		int port = Integer.parseInt(conf.getProperty(JavaV.MONGODB_PORT, "27017"));
		String dbname = conf.getProperty(JavaV.MONGODB_DBNAME, "device");

		mongDbConf.setDbHost(host);
		mongDbConf.setDbPort(port);
		mongDbConf.setDbName(dbname);

		//fileConf = new FileAccessorConf();
		//fileConf.setDateFormate("yyyy-MM-dd_HH_mm_ss");
	}

	public void start() {
		dbPa = PersistenceAccessorFactory.getAccessor(mongDbConf);
		//filePa = PersistenceAccessorFactory.getAccessor(fileConf);
		if (dbPa == null) {
			throw new NullPointerException("DbPersistence or FilePersistence initial failed");
		}
	}

	public void stop() {
		// now this will not throw exception so stop it first
		if (filePa != null) {
			try {
				filePa.close();
			} catch (Exception e) {
				log.warn("Stop file accessor error", e);
			}
		}
		filePa = null;

		if (dbPa != null) {
			try {
				dbPa.close();
			} catch (Exception e) {
				log.warn("Stop persist accessor error", e);
			}
		}
		dbPa = null;

	}

	@Override
	public void persistence(PersistenceData pd, PersistenceOpt popt) throws Exception {
		if (pd != null && popt != null) {
			try {
				log.info("first try to persist datas to persist");
				dbPa.persistenceOne(popt, pd);
				log.info("successfully persisted to persist");
			} catch (Exception e) {
				log.warn("persist to persist error", e);
				PersistenceOpt backupOpt = popt.backupOpt();
				if (backupOpt != null) {
					log.info("Try to persist to backup");
					filePa.persistenceOne(backupOpt, pd);
					log.info("successfully persisted to file");
				} else {
					log.warn("No backup is enabled, unpersistted datas are dropped");
				}
			}
		} else {
			log.warn("Cannot persist when persistence data is null or persistence opt is null");
		}
	}

	@Override
	public void persistence(Collection<PersistenceData> pds, PersistenceOpt popt) throws Exception {
		int batchSize = pds.size();
		log.info("batch size is: " + pds.size());
		// if pdata is empty it will cause
		// "java.lang.IllegalArgumentException: state should be: writes is not an empty list" exception
		// in mongo insertMany method
		if (pds != null && popt != null) {
			try {
				log.info("first try to persist datas to persist");
				dbPa.persistenceBatch(popt, pds);
				log.info("successfully persisted to persist");
			} catch (Exception e) {
				log.warn("persist to persist error", e);
				PersistenceOpt backupOpt = popt.backupOpt();
				if (backupOpt != null) {
					log.info("Try to persist to backup");
					filePa.persistenceBatch(backupOpt, pds);
					log.info("successfully persisted to file");
				} else {
					log.warn("No backup is enabled, unpersistted datas are dropped");
				}

			}
		} else {
			log.warn("Cannot persist batch when persistence data is empty or persistence opt is null");
		}

	}

}
