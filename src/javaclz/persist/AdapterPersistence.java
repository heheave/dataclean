package javaclz.persist;

import javaclz.JavaV;
import javaclz.persist.accessor.PersistenceAccessor;
import javaclz.persist.accessor.PersistenceAccessorFactory;
import javaclz.persist.config.DbAccessorConf;
import javaclz.persist.config.FileAccessorConf;
import javaclz.persist.data.PersistenceData;
import javaclz.persist.opt.PersistenceOpt;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import javaclz.persist.accessor.PersistenceAccessorFactory.DBTYPE;
import java.util.Collection;
import java.util.Properties;

public class AdapterPersistence implements Persistence {

	private static final Logger log = Logger.getLogger(AdapterPersistence.class);

	private final Properties conf;

	private final DbAccessorConf mongDbConf;

	private final FileAccessorConf fileConf;

	private PersistenceAccessor dbPa;

	private PersistenceAccessor filePa;

	public AdapterPersistence(Configuration hconf, Properties conf) {
		this.conf = conf;
		mongDbConf = new DbAccessorConf();
		mongDbConf.setDbType(DBTYPE.MONGO);
		String host = this.conf.getProperty(JavaV.PERSIST_MONGODB_HOST, "localhost");
		int port = Integer.parseInt(this.conf.getProperty(JavaV.PERSIST_MONGODB_PORT, "27017"));
		String dbname = this.conf.getProperty(JavaV.PERSIST_MONGODB_DBNAME, "device");
		mongDbConf.setDbHost(host);
		mongDbConf.setDbPort(port);
		mongDbConf.setDbName(dbname);

		String fileDateFormat = this.conf.getProperty(JavaV.PERSIST_FILE_DATE_FORMAT, "yyyy-MM-dd_HH_mm_ss");
		fileConf = new FileAccessorConf(hconf);
		fileConf.setDateFormat(fileDateFormat);
	}

	public void start() {
		dbPa = PersistenceAccessorFactory.getAccessor(mongDbConf);
		filePa = PersistenceAccessorFactory.getAccessor(fileConf);
		if (dbPa == null) {
			log.warn("db persistence accessor init error");
		}

		if (filePa == null) {
			log.warn("fs persistence accessor init error");
		}
	}

	public void stop() {
		// now this will not throw exception so stop it first
		if (filePa != null) {
			try {
				filePa.close();
			} catch (Exception e) {
				log.warn("Stop file accessor error", e);
			} finally {
				filePa = null;
			}
		}

		if (dbPa != null) {
			try {
				dbPa.close();
			} catch (Exception e) {
				log.warn("Stop persist accessor error", e);
			} finally {
				dbPa = null;
			}
		}

	}

	@Override
	public void persistence(PersistenceData pd, PersistenceOpt popt, PersistenceLevel plevel) throws Exception {
		boolean isBackup;
		if (plevel.isMain()) {
			if (dbPa != null && pd != null && popt != null) {
				try {
					log.info("Try to persist data to persist");
					dbPa.persistenceOne(popt, pd);
					log.info("Successfully persisted to main");
					isBackup = plevel.isForce() && plevel.isBackup();
				} catch (Exception e) {
					isBackup = plevel.isBackup();
					log.info("Try to persist to backup? " + isBackup);
				}
			} else {
				throw new NullPointerException("Main persistence is enabled, but null occurred");
			}
		} else {
			isBackup = plevel.isBackup();
		}

		if (isBackup) {
			PersistenceOpt backupOpt = popt.backupOpt();
			if (filePa != null && pd != null && backupOpt != null) {
				log.info("Try to persist to backup");
				filePa.persistenceOne(backupOpt, pd);
				log.info("Successfully persisted to backup");
			} else {
				throw new NullPointerException("Backup persistence is enabled, but null occurred");
			}
		}
	}

	@Override
	public void persistence(Collection<PersistenceData> pds, PersistenceOpt popt, PersistenceLevel plevel) throws Exception {
		boolean isBackup;
		if (plevel.isMain()) {
			if (dbPa != null && pds != null && popt != null) {
				try {
					log.info("Try to persist datas to persist");
					dbPa.persistenceBatch(popt, pds);
					log.info("Successfully persisted to main");
					isBackup = plevel.isForce() && plevel.isBackup();
				} catch (Exception e) {
					isBackup = plevel.isBackup();
					log.info("Try to persist to backup? " + isBackup);
				}
			} else {
				throw new NullPointerException("Main persistence is enabled, but null occurred");
			}
		} else {
			isBackup = plevel.isBackup();
		}

		if (isBackup) {
			PersistenceOpt backupOpt = popt.backupOpt();
			if (filePa != null && pds != null && backupOpt != null) {
				log.info("Try to persist to backup");
				filePa.persistenceBatch(backupOpt, pds);
				log.info("Successfully persisted to backup");
			} else {
				throw new NullPointerException("Backup persistence is enabled, but null occurred");
			}
		}
	}

}
