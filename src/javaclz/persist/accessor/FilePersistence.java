package javaclz.persist.accessor;

import javaclz.persist.opt.PersistenceOpt;
import javaclz.persist.data.PersistenceData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class FilePersistence implements ModulePersistence {
	
	private static final Logger log = LoggerFactory.getLogger(MongoPersistence.class);

    private static final SimpleDateFormat dbVersionSdf = new SimpleDateFormat("yyyy-MM-dd");

	private final FileSystem fs;

	private final SimpleDateFormat sdf;

	public FilePersistence(Configuration conf, String dateFormat) throws IOException {
		sdf = new SimpleDateFormat(dateFormat);
		if (conf == null) {
			conf = new Configuration();
		}
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			log.warn("Could not get hdfs FileSystem", e);
			throw e;
		}
	}

	@Override
	public synchronized void persistenceOne(PersistenceOpt opt, PersistenceData data)  throws Exception{
		List<PersistenceData> dataList = new ArrayList<PersistenceData>(1);
		dataList.add(data);
		persistenceBatch(opt, dataList);
	}

	@Override
	public synchronized void persistenceBatch(PersistenceOpt opt, Collection<PersistenceData> data) throws Exception{
		if (data.isEmpty()) {
			return;
		}
		String basePath = opt.getStr1();
		if (basePath == null) {
			log.warn("Unspecify file base path, data cannot be persisisted to file but all droped");
			return;
		}
		Path tblPath = new Path(basePath, tblVersion());
        fs.mkdirs(tblPath);
		OutputStream dos = null;
		PrintWriter pw = null;
		try {
            Path tryPath = new Path(tblPath, sdf.format(System.currentTimeMillis()));
            if (isLocal()) {
                dos = new FileOutputStream(tryPath.toUri().getPath(), true);
            } else {
                try {
                    dos = fs.create(tryPath, false);
                } catch (IOException ioe) {
                    dos = fs.append(tryPath);
                }
            }
			pw = new PrintWriter(dos, true);
			for (PersistenceData pd: data) {
				if (pd.toJson() != null) {
					pw.println(pd.toJson().toString());
				}
			}
			pw.flush();
		} catch (IOException e) {
			log.error("CREATE FILE ERROR", e);
		} finally {
			if (dos!=null) {
				dos.close();
			}

			if (pw != null) {
				pw.close();
			}
		}
	}

	private String tblVersion() {
        return dbVersionSdf.format(new Date(System.currentTimeMillis()));
	}

    private boolean isLocal() {
        return fs instanceof LocalFileSystem;
    }

	@Override
	public void close(){
		log.info("File persistence close at " + System.currentTimeMillis());
		try {
			fs.close();
		} catch (IOException e) {
			log.info("Close file system error", e);
		}
	}

}
