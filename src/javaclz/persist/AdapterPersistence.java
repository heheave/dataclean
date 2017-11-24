package javaclz.persist;

//import conf.DeviceConfigMananger;
//import conf.NodeHook;
import javaclz.JavaV;
import javaclz.persist.accessor.ModulePersistence;
import javaclz.persist.accessor.PersistenceFactory;
import javaclz.persist.config.DbAccessorConf;
import javaclz.persist.config.FileAccessorConf;
import javaclz.persist.data.PersistenceData;
import javaclz.persist.opt.PersistenceOpt;
import org.apache.hadoop.conf.Configuration;
import javaclz.persist.accessor.PersistenceFactory.DBTYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AdapterPersistence implements Persistence {

	private static final Logger log = LoggerFactory.getLogger(AdapterPersistence.class);

	private final Properties conf;

	private final String defaultDbName;

	private final DbAccessorConf mongDbConf;

	private final FileAccessorConf fileConf;

	private Map<String, ModulePersistence> dbPas;

	private ModulePersistence filePa;

	private final String[] hookIds;

	public AdapterPersistence(Configuration hconf, Properties conf) {
		this.conf = conf;
		mongDbConf = new DbAccessorConf();
		mongDbConf.setDbType(DBTYPE.MONGO);
		String host = this.conf.getProperty(JavaV.PERSIST_MONGODB_HOST.key, JavaV.PERSIST_MONGODB_HOST.dv());
		int port = Integer.parseInt(this.conf.getProperty(JavaV.PERSIST_MONGODB_PORT.key, JavaV.PERSIST_MONGODB_PORT.dv()));
		defaultDbName = this.conf.getProperty(JavaV.PERSIST_MONGODB_DBNAME.key, JavaV.PERSIST_MONGODB_DBNAME.dv());
		mongDbConf.setTimeout(conf.getProperty(JavaV.PERSIST_MONGODB_TIMEOUT.key, JavaV.PERSIST_MONGODB_TIMEOUT.dv()));
		mongDbConf.setDbHost(host);
		mongDbConf.setDbPort(port);
		mongDbConf.setDbName(defaultDbName);

		//Configuration hconf = new Configuration();
		String fileDateFormat = this.conf.getProperty(JavaV.PERSIST_FILE_DATE_FORMAT.key, JavaV.PERSIST_FILE_DATE_FORMAT.dv());
		hconf.setBoolean("dfs.support.append", true);
		fileConf = new FileAccessorConf(hconf);
		fileConf.setDateFormat(fileDateFormat);

		hookIds = new String[2];
	}

	public void start() {
		dbPas = new HashMap<String, ModulePersistence>();
		ModulePersistence dbPa = PersistenceFactory.getAccessor(mongDbConf);
		dbPas.put(defaultDbName, dbPa);
		filePa = PersistenceFactory.getAccessor(fileConf);
		if (dbPa == null) {
			log.warn("db persistence accessor init error");
		}

		if (filePa == null) {
			log.warn("fs persistence accessor init error");
		}
//		hookIds[0] = DeviceConfigMananger.addNewHook(new NodeHook() {
//			@Override
//			public void trigerOn(String app, Object data) {
//				DbAccessorConf dbConf = new DbAccessorConf();
//				dbConf.setDbName(app);
//				// TODO(now only one mongo db is allow)
//				dbConf.setDbHost(mongDbConf.getDbHost());
//				dbConf.setDbPort(mongDbConf.getDbPort());
//				dbConf.setTimeout(String.valueOf(mongDbConf.getTimeout()));
//				dbConf.setDbType(mongDbConf.getDbType());
//				try {
//					addNewPersistence(dbConf);
//				} catch (Exception e) {
//					log.warn("Add persistence error: " + app);
//				} finally {
//					log.info("Add app persistence" + app + ", size: " + dbPas.size());
//				}
//			}
//		});
//
//		hookIds[1] = DeviceConfigMananger.addDeleteHook(new NodeHook() {
//			@Override
//			public void trigerOn(String app, Object data) {
//				try {
//					removePersistence(app);
//				} catch (Exception e) {
//					log.warn("Close persistence error: " + app);
//				} finally {
//					log.info("Remove app persistence" + app + ", size: " + dbPas.size());
//				}
//			}
//		});
	}

	public void stop() {
		//DeviceConfigMananger.rmNewHook(hookIds[0]);
		//DeviceConfigMananger.rmDeleteHook(hookIds[1]);
		// now this will not throw exception so stop it first
		if (filePa != null) {
			try {
				filePa.close();
			} catch (Exception e) {
				log.warn("Stop file accessor error", e);
			} finally {
				filePa = null;
			}

			synchronized (dbPas) {
				for (Map.Entry<String, ModulePersistence> entry : dbPas.entrySet()) {
					try {
						entry.getValue().close();
					} catch (Exception e) {
						log.warn("Stop db accessor error with database: " + entry.getKey(), e);
					}
				}
				dbPas.clear();
			}
		}
	}

	public void addNewPersistence(DbAccessorConf dbConf) throws Exception {
		String dbName = dbConf.getDbName();
		if (dbName != null && !dbPas.containsKey(dbName)) {
			ModulePersistence dbPa = PersistenceFactory.getAccessor(dbConf);
			if (dbPa == null) {
				throw new NullPointerException("Could not get persistence on" + dbConf.getDbName());
			}
			synchronized (dbPas) {
				dbPas.put(dbConf.getDbName(), dbPa);
			}
		}
	}

	public void removePersistence(String dbName) throws Exception {
		ModulePersistence removedPa = dbPas.remove(dbName);
		if (removedPa != null) {
			removedPa.close();
		}
	}

	@Override
	public void persistence(PersistenceData pd, PersistenceOpt popt, PersistenceLevel plevel) throws Exception {
		boolean isBackup;
		if (plevel.isMain()) {
			ModulePersistence dbPa = dbPas.get(popt.getStr1());
			if (dbPa == null) {
				dbPa = dbPas.get(defaultDbName);
			}
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
			ModulePersistence dbPa = dbPas.get(popt.getStr1());
			if (dbPa == null) {
				log.info("USING DEFAULT ...................");
				dbPa = dbPas.get(defaultDbName);
			}
			if (dbPa != null && pds != null && popt != null) {
				try {
					log.info("Try to persist datas to persist: " + popt.getStr1());
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
