package javaclz.persist.accessor;

import javaclz.persist.opt.PersistenceOpt;
import javaclz.persist.data.PersistenceData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileAccessor implements PersistenceAccessor{
	
	private static final Logger log = Logger.getLogger(FileAccessor.class);

	private final FileSystem fs;

	private final SimpleDateFormat sdf;

	public FileAccessor(Configuration conf, String dateFormat) throws IOException {
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
	public void persistenceOne(PersistenceOpt opt, PersistenceData data)  throws Exception{
		List<PersistenceData> dataList = new ArrayList<PersistenceData>(1);
		dataList.add(data);
		persistenceBatch(opt, dataList);
	}

	@Override
	public void persistenceBatch(PersistenceOpt opt, Collection<PersistenceData> data) throws Exception{
		if (data.isEmpty()) {
			return;
		}
		String basePath = opt.getStr1();
		if (basePath == null) {
			log.warn("Unspecify file base path, data cannot be persisisted to file but all droped");
			return;
		}
		String dateStr = sdf.format(System.currentTimeMillis());
		Path filePath = new Path(basePath, dateStr);
		OutputStream dos = null;
		PrintWriter pw = null;
		try {
			dos = fs.create(filePath);
			log.info("----------FS------------" + fs.exists(filePath) + " : " + filePath.toUri().getPath());
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
