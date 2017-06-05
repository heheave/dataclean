package javaclz.persist.accessor;

import javaclz.persist.opt.PersistenceOpt;
import javaclz.persist.data.PersistenceData;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileAccessor implements PersistenceAccessor{
	
	private static final Logger log = Logger.getLogger(FileAccessor.class);
	
	private final SimpleDateFormat sdf;

	public FileAccessor(String dateFormat) {
		sdf = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss");
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
		if (basePath != null) {
			log.warn("Unspecify file base path, data cannot be persisisted to file but all droped");
			return;
		}
		String dateStr = sdf.format(System.currentTimeMillis());
		File fileBase = new File(basePath);
		PrintWriter pw = null;
		try {
			if (!fileBase.exists()) {
				fileBase.mkdirs();
			}
			pw = new PrintWriter(basePath + File.separatorChar + dateStr);
			String dataStr;
			for (PersistenceData pd: data) {
				dataStr = pd.toJson() == null ? "": pd.toJson().toString();
				pw.println(dataStr);
			}
			pw.flush();
		} catch (IOException e) {
			log.error("CREATE FILE ERROR", e);
		} finally {
			if (pw!=null) {
				pw.close();
			}
		}
	}

	@Override
	public void close(){
		// TODO
	}

}
